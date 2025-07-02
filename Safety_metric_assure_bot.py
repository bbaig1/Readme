## THIS Project "Assure BOT for Metrics" sends automated Emails for Different metrics at Metric-Location level. 
## It has a feature to add n number of metrics in a google sheet, add subscribers in subcribers sheet and 
## it will automatically send alert to each indivisual based on the metric and location.

import pypostmaster
import pandas as pd
import pygsheets
import gspread
from queryrunner_client import Client
from gspread_formatting import (
    DataValidationRule,
    BooleanCondition,
    set_data_validation_for_cell_range,
    CellFormat,
    Color,
    format_cell_range
)
from datetime import datetime, timedelta
import os
import json
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import re
#from gspread_dataframe import set_with_dataframe

### 2. Initialize API Clients and Configuration Variables

secret_json = os.environ['SECRETS_PATH'] + '/''/creds' ##
gc = pygsheets.authorize(service_file=secret_json)
gc_gspread = gspread.service_account(filename=secret_json)
qr = Client(consumer_name='core_services_safety_ops', interactive=False)
#qr = Client(user_email= '') ##

yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
## all emails will be sent from this group
from_addr = "" ##
helper = pypostmaster.MailHelper()

### 3. Retrieve Master Table with Metric Values and Thresholds

# Fetch the master table containing metric values and thresholds for the previous day
database = 'presto'
yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
query = f'''select * from TABLE where date(date) = date('{yesterday}')'''
cursor = qr.execute(database, query)
df_masterdata = pd.DataFrame(cursor.fetchall(), columns=cursor.columns)

df_masterdata.rename(columns={'region': 'location'}, inplace=True)
df_masterdata.rename(columns={'l1': 'AL1'}, inplace=True)
df_masterdata.rename(columns={'l2': 'AL2'}, inplace=True)
df_masterdata.rename(columns={'l3': 'AL3'}, inplace=True)


df_masterdata

# Converting GSheet to required form
# Open the Google Sheet
Google_form_sheet_url = "https://docs.google.com/spreadsheets/d/1aFWuwV1VuUm9ejyRoZ8SN3XO9tsJgmLEXPI0-c48d7E/edit?gid=1862290530#gid=1862290530"
subs_sheet_url='https://docs.google.com/spreadsheets/d/1aFWuwV1VuUm9ejyRoZ8SN3XO9tsJgmLEXPI0-c48d7E/edit?gid=174216362#gid=174216362'

sh = gc.open_by_url(Google_form_sheet_url)

# Load Metrics from 2nd sheet (index 1)
metrics_df = sh[1].get_as_df()  
metrics_df.columns = metrics_df.columns.str.strip()

# Load Subscribers from 3rd sheet (index 2)
subs_wks = sh[2]
subs_df = subs_wks.get_as_df()
subs_df.columns = subs_df.columns.str.strip()

# Identify Metric Headers
metric_headers = metrics_df["Metric Header"].unique()

# Separate rows to keep and rows to expand
rows_to_keep = subs_df[~subs_df["Metric"].isin(metric_headers)]
rows_to_expand = subs_df[subs_df["Metric"].isin(metric_headers)]

# Expand rows using mapping from Metrics sheet
expanded_rows = []
for _, row in rows_to_expand.iterrows():
    location = str(row["location"]).strip()
    metric_header = str(row["Metric"]).strip()
    name = row["Subscriber Name"]
    email = row["Subscriber Email"]
    reason = row.get("Reason to Subscribe", "")  # Get reason safely

    matching_metrics = metrics_df[
        (metrics_df["Metric Header"] == metric_header) &
        (metrics_df["location"].str.strip() == location)
    ]

    for _, mrow in matching_metrics.iterrows():
        expanded_rows.append({
            "location": location,
            "Metric": mrow["Metric"],
            "Subscriber Name": name,
            "Subscriber Email": email,
            "Reason to Subscribe": reason
        })

# Combine kept + expanded rows
final_df = pd.concat([rows_to_keep, pd.DataFrame(expanded_rows)], ignore_index=True)
final_df = final_df.drop_duplicates()

# Write updated DataFrame back to Subscribers sheet
subs_wks.clear()
subs_wks.set_dataframe(final_df, start="A1", nan="")

print("✅ Subscribers sheet updated with correct Metric values, Metric Headers replaced, and 'Reason to Subscribe' retained!")

# Upload raw data or append depending on flag
def upload_sheet(data, sheet_name, gsheet_url, append, add_dropdown):
    try:
        sh = gc.open_by_url(gsheet_url)
        try:
            wks = sh.worksheet_by_title(sheet_name)
        except pygsheets.WorksheetNotFound:
            wks = sh.add_worksheet(sheet_name)

        if append:
            existing = wks.get_as_df()
            data = pd.concat([existing, data], ignore_index=True)
        wks.clear()
        wks.resize(rows=data.shape[0]+1, cols=data.shape[1])
        wks.set_dataframe(data, start='A1', copy_head=True)

        # Add dropdowns for specified columns
        if add_dropdown:
            sheet_id = gsheet_url.split("/d/")[1].split("/")[0]
            gsheet = gc_gspread.open_by_key(sheet_id)
            worksheet = gsheet.worksheet(sheet_name)

            for col_name in ["Alert Investigated", "Outcome"]:
                if col_name in data.columns:
                    col_idx = data.columns.get_loc(col_name) + 1
                    end_row = data.shape[0] + 1

                    if col_name == "Alert Investigated":
                        rule = DataValidationRule(
                            BooleanCondition('ONE_OF_LIST', ['Yes', 'No']),
                            showCustomUi=True,
                            strict=True
                        )
                    else:  # Outcome column
                        rule = DataValidationRule(
                            BooleanCondition('ONE_OF_LIST', ['False Positive', 'Breach Confirmed']),
                            showCustomUi=True,
                            strict=True
                        )

                    cell_range = f"{gspread.utils.rowcol_to_a1(2, col_idx)}:{gspread.utils.rowcol_to_a1(end_row, col_idx)}"
                    set_data_validation_for_cell_range(worksheet, cell_range, rule)

        return True
    except Exception as e:
        print(f"Error while uploading sheet: {e}")
        return False

### 4. Define Helper Functions for Email Alerts and Sample Sheet Management

def is_valid_email(email):
        return bool(re.match(r"^[\w\.-]+@[\w\.-]+\.\w{2,4}$", email))

def get_alert_type(row):
    if row['value'] >= row['AL3']:
        return 'AL3'
    elif row['value'] >= row['AL2']:
        return 'AL2'
    elif row['value'] > row['AL1']:
        return 'AL1'
    return None

def get_severity_and_response(alert_type):
                mapping = {
                "AL1": ("Low", "Monitor the situation"),
                "AL2": ("Medium", "Investigate the samples"),
                "AL3": ("High", "Take immediate action"),
                }
                return mapping.get(alert_type, ("N/A", "N/A"))

# Logic to update samples Gsheet and only keep 5 for each metric
def upload_sample_sheet(data, base_sheet_name, gsheet_url):
    try:
        # 1. Open the target Google Sheet by URL
        sh = gc.open_by_url(gsheet_url)

        # 2. Generate yesterday's date for sheet naming
        full_sheet_name = f"Samples {base_sheet_name} {yesterday}"

        # 3. Get all worksheets that match the current metric's base name
        all_sheets = sh.worksheets()
        sample_sheets = [s for s in all_sheets if s.title.startswith(f"Samples {base_sheet_name}")]

        # 4. Sort these sheets by date in their title to find the oldest
        sample_sheets_sorted = sorted(
            sample_sheets,
            key=lambda ws: datetime.strptime(ws.title.split()[-1], "%Y-%m-%d")
        )

        # 5. Decide tab retention limit based on the metric type
        retention_limit = 20 if base_sheet_name.startswith("Samples Trips") else 5

        # 6. If limit exceeded, delete oldest tabs beyond retention limit
        while len(sample_sheets_sorted) >= retention_limit:
            sh.del_worksheet(sample_sheets_sorted.pop(0))  # Remove and delete the oldest

        # 7. Delete today's sheet if it already exists to avoid duplication
        for sheet in sample_sheets_sorted:
            if sheet.title == full_sheet_name:
                sh.del_worksheet(sheet)
                break

        # 8. Add a new worksheet and write the sample data to it
        wks = sh.add_worksheet(full_sheet_name)
        wks.set_dataframe(data, start='A1', copy_head=True)

        # 9. Set tab color to yellow using Google Sheets API
        sheet_id = wks.spreadsheet.id
        sheet_gid = wks.id
        creds = Credentials.from_service_account_file(secret_json, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        service = build('sheets', 'v4', credentials=creds)
        requests = [{
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_gid,
                    "tabColor": {
                        "red": 1.0,
                        "green": 1.0,
                        "blue": 0.0
                    }
                },
                "fields": "tabColor"
            }
        }]
        service.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={"requests": requests}
        ).execute()

        # 10. Generate direct link to the newly created worksheet tab using its gid
        sheet_gid_url = f"{gsheet_url}#gid={sheet_gid}"

        # 11. Print confirmation and return the specific tab URL
        print(f"Uploaded sample to: {full_sheet_name}")
        print(sheet_gid_url)
        return sheet_gid_url

    except Exception as e:
        # 12. Handle and print any upload errors
        print(f"Error uploading sample sheet {base_sheet_name}: {e}")
        return None

def send_daily_alert(df, metric_to_data_map, subs_sheet_url):
    df_yday = df[df["date"] == yesterday].copy()
    df_yday['alert_type'] = df_yday.apply(get_alert_type, axis=1)
    df_breaches = df_yday[df_yday['alert_type'].notnull()].copy()

    if df_breaches.empty:
        print(f"No threshold breaches on {yesterday}. No email sent.")
        return

    # Load subscriber data
    sh = gc.open_by_url(subs_sheet_url)
    wks = sh.worksheet_by_title("Subscribers")
    subs_df = wks.get_as_df()
    subs_df.dropna(subset=["Metric", "location", "Subscriber Email"], inplace=True)

    email_log_data = [] 
    uploaded_sample_links = {}

    # Upload sample sheet once per metric and store the link
    for metric in df_breaches["metric"].unique():
        info = metric_to_data_map.get(metric, {})
        data = info.get("data")
        gsheet_url = info.get("sheet_url")
        sheet_name = info.get("sheet_name", "RawData")

        if data is not None and gsheet_url != "N/A":
            link = upload_sample_sheet(data, sheet_name, gsheet_url)
        else:
            link = "N/A"

        uploaded_sample_links[metric] = link

    # Loop through each (metric, region) and send mail
    for (metric, location), group_df in df_breaches.groupby(["metric", "location"]): ##1
        subs = subs_df[(subs_df["Metric"] == metric) & (subs_df["location"] == location)] ##2
        all_emails = subs["Subscriber Email"].dropna().unique().tolist()
        valid_emails = [e for e in all_emails if is_valid_email(e)]
        invalid_emails = [e for e in all_emails if not is_valid_email(e)]

        if invalid_emails:
            print(f"⚠️ Ignored invalid email(s) for {metric} - {location}: {invalid_emails}")

        emails = valid_emails
        if not emails:
            print(f"⚠️ No subscribers for metric: {metric}, location: {location}")
            continue

        to_email = emails[0]
        cc_emails = emails[1:]

        # Get summary sheet URLs
        summary_url = metric_to_data_map.get(metric, {}).get("sheet_url", "N/A")
        if summary_url == "N/A":
            print(f"⚠️ No summary_url for metric: {metric}")
            continue

        try:
            summary_sheet = gc.open_by_url(summary_url)
            def get_sheet_gid_by_name(sheet, name):
                for ws in sheet.worksheets():
                    if ws.title == name:
                        return ws.id
                return None

            latest_gid = get_sheet_gid_by_name(summary_sheet, "Latest Breach")
            prev_gid = get_sheet_gid_by_name(summary_sheet, "Previous Breaches")
            latest_link = f"{summary_url}#gid={latest_gid}"
            prev_link = f"{summary_url}#gid={prev_gid}"

        except Exception as e:
            print(f"❌ Error processing {metric}: {e}")
            continue

        sample_link = uploaded_sample_links.get(metric, "N/A")
        link_html = f'<a href="{sample_link}" target="_blank">View</a>' if sample_link != "N/A" else "N/A"

        rows_html = ""
        for _, row in group_df.iterrows():
            severity, response = get_severity_and_response(row['alert_type'])
            rows_html += f"""
            <tr>
                <td>{metric}</td>
                <td>{location}</td>
                <td>{row['value']}</td>
                <td>{row['alert_type']} threshold ({round(row['AL' + row['alert_type'][-1]], 2)})</td>
                <td>{severity}</td>
                <td>{response}</td>
                <td>{link_html}</td>
            </tr>
            """

            email_log_data.append({
                "Date": yesterday,
                "Metric": metric,
                "location": location, ##3
                "Value": row["value"],
                "AL1 Threshold": round(row['AL1'], 2),
                "AL2 Threshold": round(row['AL2'], 2),
                "AL3 Threshold": round(row['AL3'], 2),
                "Alert Type": row["alert_type"],
                "Alert Investigated": "",
                "Outcome": "",
                "Reviewed By": "",
                "Comments": ""
            })
        #Print(row)
        body = f"""
        <html>
          <body>
            <p>Hi team,</p>
            <p>The following threshold breaches were detected on <b>{yesterday}</b> for <b>{metric}</b> - <b>{location}</b>:</p>
            <table border="1" cellpadding="5" cellspacing="0">
              <tr>
                <th>Metric</th>
                <th>Location</th>  
                <th>Value</th>
                <th>Threshold</th>
                <th>Severity</th>
                <th>Suggested Response</th>
                <th>Samples</th>
              </tr>
              {rows_html}
            </table>
            <br>
            <p><b>Latest Breaches:</b> <a href="{latest_link}" target="_blank">View Tab</a><br>
               <b>All Previous Breaches:</b> <a href="{prev_link}" target="_blank">View Tab</a></p>
          </body>
        </html>
        """

        alert_type = group_df.iloc[0]["alert_type"]
        subject = f"{alert_type} Threshold ALERT: {metric} breach detected in {location} on {yesterday}"

        print("=" * 60)
        print("Sending Email:")
        print(f"To: {to_email}")
        print(f"CC: {cc_emails}")
        print(f"Subject: {subject}")
        print(group_df[["metric", "location", "value", "alert_type"]].to_string(index=False)) ##5
        print("=" * 60)

        helper.sendmail(from_addr, [to_email], subject, body, cc_emails)

    # Upload summary to correct sheets
    email_log_df = pd.DataFrame(email_log_data)
    if email_log_df.empty:
        print("⚠️ No emails sent, skipping summary sheet uploads.")
        return

    for metric, group_df in email_log_df.groupby("Metric"):
        summary_url = metric_to_data_map.get(metric, {}).get("sheet_url", "N/A")
        if summary_url == "N/A":
            print(f"⚠️ No summary sheet configured for metric: {metric}")
            continue

        upload_sheet(group_df, "Latest Breach", summary_url, append=False, add_dropdown=True)
        upload_sheet(group_df, "Previous Breaches", summary_url, append=True, add_dropdown=True)
        # print(summary_url)
        # print(group_df)



### 5. Process Threshold Breaches and Dispatch Daily Alerts

#df_samples['region']


query = f'''select * from TABLE where date(date) = date('{yesterday}')'''
cursor = qr.execute(database, query)
df_samples = pd.DataFrame(cursor.fetchall(), columns=cursor.columns)
df_samples.rename(columns={'region': 'location'}, inplace=True)
#Breaches list from Master Data
df_masterdata['alert_type'] = df_masterdata.apply(get_alert_type, axis=1)
df_breaches = df_masterdata[df_masterdata['alert_type'].notnull()].copy()
# df_breaches

metric_to_data_map = {}

for metric, metric_temp in zip(df_breaches['metric'].str.lower().str.replace(' ', '_').str.replace('temporary', 'Temporary').str.replace('permanent', 'Permanent'), df_breaches['metric']):
    #Retreieving Samples
    json_df = df_samples[df_samples['metric'].str.contains(metric) | df_samples['metric'].str.contains(metric_temp)]['json_supporting_information'].apply(json.loads).apply(pd.Series)
    df_samples_clean = df_samples[df_samples['metric'].str.contains(metric) | df_samples['metric'].str.contains(metric_temp)].drop(columns=['json_supporting_information'])
    df_samples_temp = pd.concat([df_samples_clean, json_df], axis=1)
    #Populating Samples
    matches = metrics_df.loc[metrics_df['Metric'].str.lower().str.replace(' ', '_').str.replace('temporary', 'Temporary').str.replace('permanent', 'Permanent') == metric, 'Latest Breach link']
    if not matches.empty:
        gsheet_url = matches.iloc[0]
    else:
        gsheet_url = None 
    # upload_sample_sheet(df_samples_temp, metric, gsheet_url)
    metric_to_data_map[metric_temp] = {
                "sheet_url" : gsheet_url,
                "sheet_name" : metric_temp,
                "data" : df_samples_temp
}




send_daily_alert(df_masterdata, metric_to_data_map, subs_sheet_url)

### 6. Update 'Readme' Tab in Each Google Sheet with Index of Relevant Tabs

# Updating the Readme tab After Uploading and Updating the Sheet
def update_index_tab(sheet_id, index_tab_name="Readme"):

    # Open the Google Sheet
    sh = gc.open_by_key(sheet_id)

    # Get all worksheets and filter out the index tab
    all_sheets = sh.worksheets()
    filtered_sheets = [ws for ws in all_sheets if ws.title not in [index_tab_name, "Previous Breaches", "Latest Breach"]]

    # Sort remaining sheets by title (assuming date format like "2024-04-27")
    filtered_sheets.sort(key=lambda ws: ws.title, reverse=True)

    # Take latest 4
    latest_sheets = filtered_sheets[:4]

    # Try to get fixed sheets
    fixed_tabs = []
    for fixed_title in ["Previous Breaches", "Latest Breach"]:
        try:
            ws = sh.worksheet_by_title(fixed_title)
            fixed_tabs.append(ws)
        except pygsheets.WorksheetNotFound:
            print(f"⚠️ Sheet '{fixed_title}' not found. Skipping it.")

    # Get or create the index sheet
    try:
        index_ws = sh.worksheet_by_title(index_tab_name)
    except pygsheets.WorksheetNotFound:
        index_ws = sh.add_worksheet(index_tab_name, rows=100, cols=5)

    # Get existing values to find first empty row
    existing_values = index_ws.get_all_values()
    first_empty_row = len(existing_values) + 2  # to leave gap from existing content

    # Header
    header = [["Sr No", "Sheet Name", "Description", "Link"]]
    rows = []

    # Combine fixed + latest sheets
    combined_sheets = fixed_tabs + latest_sheets

    # Construct rows with serial numbers
    for idx, ws in enumerate(combined_sheets, start=1):
        title = ws.title
        gid = ws.id
        description = f"This sheet contains {title}" #if title in ["Previous Breaches", "Latest Breach"] else f"This sheet contains {title} for the date {title}"
        link_formula = f'=HYPERLINK("https://docs.google.com/spreadsheets/d/{sheet_id}/edit#gid={gid}", "{title}")'
        rows.append([idx, title, description, link_formula])

    # Update values
    cell_address = 'C11'
    index_ws.update_values(cell_address, header + rows)

    print("✅ Index tab updated with fixed tabs and latest 4 sheets.")

updated_sheet_id = []
for sheet_link in metrics_df['Latest Breach link']:
    sheet_id = re.search(r'/d/([a-zA-Z0-9-_]+)', sheet_link).group(1)
    if sheet_id not in updated_sheet_id:
        update_index_tab(sheet_id, index_tab_name="Readme")
        updated_sheet_id.append(sheet_id)

### 7. Send Reminder Emails for Unacknowledged Breaches Older Than 7 Days

# # Sending alerts to all recepeints for that metric who has not responded in 7 days
# # Configuration
# summary_sheet_map = {}
# for metric, sheet_link in zip(metrics_df['Metric'], metrics_df['Latest Breach link']):
#     sheet_id = re.search(r'/d/([a-zA-Z0-9-_]+)', sheet_link).group(1)
#     sheet_metadata = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
#     sheets = sheet_metadata['sheets']
#     previous_breaches_tab = sheets[2]
#     sheet_gid = previous_breaches_tab['properties']['sheetId']
#     summary_sheet_map[metric] = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit#gid={sheet_gid}"

# def read_sheet(sheet_url_or_id, sheet_name):
#     if "https://" in sheet_url_or_id:
#         sh = gc.open_by_url(sheet_url_or_id)
#     else:
#         sh = gc.open_by_key(sheet_url_or_id)
#     wks = sh.worksheet_by_title(sheet_name)
#     df = wks.get_as_df()
#     return df

# def clean_sheet_id(url):
#     return url.split("/d/")[1].split("/edit")[0]

# # Read Subscribers
# subscribers_df = read_sheet(subs_sheet_url, sheet_name='Subscribers')
# subscribers_df = subscribers_df[['Region', 'Metric', 'Subscriber Email']].dropna()
# subscribers_df = subscribers_df[subscribers_df['Subscriber Email'].apply(is_valid_email)]

# # Main Process
# for metric, sheet_url in summary_sheet_map.items():
#     try:
#         sheet_id = clean_sheet_id(sheet_url)
#         previous_breaches_df = read_sheet(sheet_id, sheet_name="Previous Breaches")

#         if 'Metric' not in previous_breaches_df.columns or 'Region' not in previous_breaches_df.columns:
#             print(f"⚠️ Missing Metric/Region in Previous Breaches sheet for: {metric}")
#             continue

#         previous_breaches_df = previous_breaches_df.dropna(subset=['Metric', 'Region'])
#         previous_breaches_df['Date'] = pd.to_datetime(previous_breaches_df['Date'], errors='coerce')

#         today = datetime.today()
#         overdue_breaches_df = previous_breaches_df[
#             (previous_breaches_df['Metric'] == metric) &
#             (previous_breaches_df['Date'] == today - timedelta(days=7)) &
#             ((previous_breaches_df['Alert Investigated'].isna()) | (previous_breaches_df['Alert Investigated'] == '')) &
#             ((previous_breaches_df['Outcome'].isna()) | (previous_breaches_df['Outcome'] == ''))
#         ]

#         if overdue_breaches_df.empty:
#             continue

#         for region in overdue_breaches_df['Region'].unique():
#             region_breaches = overdue_breaches_df[overdue_breaches_df['Region'] == region]

#             if region_breaches.empty:
#                 continue

#             overdue_breaches_html = region_breaches.to_html(index=False, border=1, classes="breach-table", justify="center")

#             html_style = """
#                 <style>
#                     .breach-table {
#                         width: 100%;
#                         border-collapse: collapse;
#                         margin: 20px 0;
#                         font-family: Arial, sans-serif;
#                     }
#                     .breach-table th, .breach-table td {
#                         padding: 10px;
#                         text-align: left;
#                         border: 1px solid #ddd;
#                     }
#                     .breach-table th {
#                         background-color: #f4f4f4;
#                         font-weight: bold;
#                     }
#                     .breach-table td {
#                         background-color: #f9f9f9;
#                     }
#                     .breach-table tr:nth-child(even) td {
#                         background-color: #f2f2f2;
#                     }
#                 </style>
#             """

#             body_html = f"""
#             <html>
#                 <head>{html_style}</head>
#                 <body>
#                     <p>Hi,</p>
#                     <p>This is a reminder that it has been more than 7 days since the following breach was raised for <strong>{metric} - {region}</strong>, and no response has been provided yet.</p>
#                     <p>Please review and update it accordingly.</p>
#                     <p><strong>Pending Breaches:</strong></p>
#                     {overdue_breaches_html}
#                     <p><a href="{sheet_url}">Click here to review the full details in the sheet</a></p>
#                     <p>Thanks,<br>Assure Bot</p>
#                 </body>
#             </html>
#             """

#             # Get all valid subscribers for this (metric, region) or (metric, Global)
#             subscribers = subscribers_df[
#                 (subscribers_df['Metric'] == metric) &
#                 ((subscribers_df['Region'] == region) | (subscribers_df['Region'] == 'Global'))
#             ]['Subscriber Email'].dropna().unique().tolist()

#             if not subscribers:
#                 print(f"⚠️ No subscribers found for metric: {metric} - {region}")
#                 continue

#             to_email = subscribers[0]
#             cc_emails = subscribers[1:]
#             helper.sendmail(
#                 from_addr,
#                 [to_email],
#                 f"Reminder: Action Needed on {metric} - {region} (Pending >7 days)",
#                 body_html,
#                 cc_emails,
#                 []
#             )

#             print(f"✅ Reminder sent to {to_email}{cc_emails} for {metric} - {region}")

#     except Exception as e:
#         print(f"❌ Error processing metric {metric}: {e}")
