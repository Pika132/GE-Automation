# Load the sheet into a pandas DataFrameimport pycountry
import re
import pycountry
import gspread
from google.oauth2 import service_account
import pandas as pd
from flask import Flask, request, render_template, Response
import PyPDF2
import io
import csv
import os
import json
import logging 

app = Flask(__name__)

scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Load credentials from environment variable
credentials_json = json.loads(os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON"))
credentials = service_account.Credentials.from_service_account_info(credentials_json, scopes=scope)
client = gspread.authorize(credentials)

# Open your Google Sheet
spreadsheet = client.open_by_url(
    "https://docs.google.com/spreadsheets/d/1THXb-qxNYQQ-13UuDxKUKM168qn7TqvkyDemh9hcbiI/edit?gid=0"
)
sheet = spreadsheet.sheet1

# Load the sheet into a pandas DataFrame
headers = sheet.row_values(1)  # Get the first row (header row)

# Remove duplicates and empty headers
headers = [header for header in headers if header.strip()]  # Remove empty headers
headers = list(dict.fromkeys(headers))  # Remove duplicates while preserving order

data = sheet.get_all_records(expected_headers=headers)

df = pd.DataFrame(data)

# Create a lookup dictionary for all UN-related info
un_lookup = {
    row['GE Item Number']: {
        'UN Number': row.get('UN Number', ''),
        'IATA UN Hazard Class': row.get('IATA UN Hazard Class', ''),
        'Packing Group': row.get('Packing Group', ''),
        'IATA Packing Instructions': row.get('IATA Packing Instructions', ''),
        'UN Description': row.get('UN Description', '')
    }
    for _, row in df.iterrows()
}

# Country code mapping using pycountry and adding specific codes
country_code_mapping = {country.alpha_2: country.name for country in pycountry.countries}
country_code_mapping.update({'US': 'USA', 'TW': 'Taiwan', 'KR': 'Korea'})

# Function to replace country codes with country names
def replace_country(match):
    country_code = match.group(0)  # Extract the country code (2-letter code)
    
    # Get the country name from the mapping or return the code itself if not found
    country_name = country_code_mapping.get(country_code, country_code)
    
    if country_code == 'US':
        return country_name  # Special case for "US" -> "USA"
    return f"{country_code} {country_name}"

def merge_boxes(boxes):
    if not boxes:
        return []

    merged = []
    current = boxes[0].copy()
    current['Total Boxes'] = 1  # initialize

    for b in boxes[1:]:
        # Check if Units and Weight are the same as current
        if b['Units'] == current['Units'] and b['Weight'] == current['Weight']:
            current['Total Boxes'] += 1
        else:
            merged.append(current)
            current = b.copy()
            current['Total Boxes'] = 1
    merged.append(current)
    return merged

def extract_item_numbers(text):
    """
    Extract all item numbers:
    - First try after EA
    - Fallback: after DMQ if EA not found
    - Normalize: remove any letter prefixes like DMQ
    """
    # 1️⃣ First try: after EA
    pattern_ea = r"\d+\s*EA\s*([A-Z]*\s*\d[\d\-]*)"
    matches = re.findall(pattern_ea, text, flags=re.IGNORECASE)
    
    # 2️⃣ Fallback: after DMQ if nothing found
    if not matches:
        pattern_dmq = r"\d+\s*DMQ\s*([A-Z]*\s*\d[\d\-]*)"
        matches = re.findall(pattern_dmq, text, flags=re.IGNORECASE)

    # Normalize: remove any letter prefixes like DMQ
    clean_matches = [re.sub(r"^[A-Z]+\s*", "", m).strip() for m in matches]
    
    return clean_matches

def extract_packages_and_weight(text):
    """
    Extract NO OF PACKAGES and NET WEIGHT (KG) from the last line
    of the PDF table, even if headers are merged.
    """
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return None, None

    # Use the last non-empty line
    last_line = lines[-1]

    # Extract all numbers (integers or decimals)
    numbers = re.findall(r"\d+(?:\.\d+)?", last_line)
    
    if len(numbers) >= 2:
        total_containers = numbers[0]       # first number = NO OF PACKAGES
        net_weight = numbers[-2]          # second-to-last number = NET WEIGHT (KG)
        return total_containers, net_weight
    
    return None, None

# Extract the information from the text
def extract_info(text):
    info = {}
    info['Detected Format'] = None  # Will store which format was used

    # --- Attempt Format 1 (Delivery:) ---
    delivery_match = re.search(r'Delivery\s*:\s*(\d+)', text)
    if delivery_match:
        info['Delivery'] = delivery_match.group(1)

        # --- Ship To address ---
        ship_to_match = re.search(r'Ship To:\s*(.*?)(?=\s*Ship From:)', text, re.DOTALL)
        if ship_to_match:
            ship_to = ship_to_match.group(1)
            ship_to_lines = [line.rstrip() for line in ship_to.splitlines() if line.strip()]
            last_line = ship_to_lines[-1]
            pattern = re.compile(r'\b[A-Z]{2}\b', re.IGNORECASE)
            last_line = pattern.sub(replace_country, last_line)
            ship_to_lines[-1] = last_line
            info['Ship To'] = "\n".join(ship_to_lines)

        # --- Totals ---
        containers_match = re.search(r'Total number of containers\s*:\s*(\d+)', text)
        total_containers = int(containers_match.group(1)) if containers_match else 0
        info['Total Containers'] = total_containers

        qty_match = re.search(r'Total Qty/LPN:\s*([\d.]+)', text)
        total_items = float(qty_match.group(1)) if qty_match else 0
        info['Total Qty/LPN'] = total_items

        weight_match = re.search(r'Net Weight\(kg\):\s*([\d.]+)', text)
        total_weight = float(weight_match.group(1)) if weight_match else 0
        info['Net Weight (kg)'] = total_weight

        # --- Item numbers ---
        clean_text = re.sub(r'[^\x00-\x7F]+', '', text)
        item_numbers = re.findall(r'^\s*as([^\s]*)', clean_text, re.MULTILINE | re.IGNORECASE)
        info['Item Numbers'] = item_numbers
    else:
        # Extract delivery/invoice
        invoice = re.search(r"INVOICE\s*NO[:\s]*([^\s]+)", text, re.IGNORECASE)
        delivery = invoice.group(1) if invoice else None
        info['Delivery'] = delivery


        # Extract consignee
        consignee_match = re.search(
            r"CONSIGNEE\s+(.*?\n[A-Z]{2}\s*$)",
            text,
            re.DOTALL | re.MULTILINE
        )
        if consignee_match:
            consignee_lines = [line.rstrip() for line in consignee_match.group(1).splitlines() if line.strip()]
            if consignee_lines:
                last_line = consignee_lines[-1]
                last_line = re.sub(r'\b[A-Z]{2}\b', replace_country, last_line)
                consignee_lines[-1] = last_line
            info["Ship To"] = "\n".join(consignee_lines)
        else:
            info["Ship To"] = None

        # Total items
        qty = re.search(r"\b(\d+)\s*EA\b", text)
        if not qty:
            qty = re.search(r"\b(\d+)\s*[xX]\b", text)
        total_items = float(qty.group(1)) if qty else 0
        info['Total Qty/LPN'] = total_items

        # Containers & weight
        total_containers, net_weight = extract_packages_and_weight(text)
        info["Total Containers"] = int(total_containers) if total_containers else 0
        total_containers = int(total_containers) if total_containers else 0 
        info["Net Weight (kg)"] = float(net_weight) if net_weight else 0
        total_weight = float(net_weight) if net_weight else 0

        # Item numbers
        item_numbers = extract_item_numbers(text)

        info["Item Numbers"] = item_numbers

    # --- Split items into boxes ---
    def split_into_boxes(total_items, total_weight, total_containers):
        boxes = []
        if total_containers == 0:
            return boxes

        base_units = int(total_items // total_containers)
        remainder = int(total_items % total_containers)

        for i in range(total_containers):
            units = base_units + (1 if i < remainder else 0)
            weight = round(total_weight * (units / total_items), 2) if total_items > 0 else 0
            boxes.append({
                'Box': i + 1,
                'Units': units,
                'Weight': weight
            })
        return boxes

    # ✅ Split then merge boxes
    info['Boxes'] = split_into_boxes(total_items, total_weight, total_containers)
    info['Boxes'] = merge_boxes(info['Boxes'])

    # --- Aggregate totals ---
    info['Total Boxes'] = len(info['Boxes'])
    info['Total Units'] = sum(b['Units'] for b in info['Boxes'])
    info['Total Weight'] = sum(b['Weight'] for b in info['Boxes'])

    # --- Lookup UN info ---
    info['UN Number'] = []
    info['IATA UN Hazard Class'] = []
    info['Packing Group'] = []
    info['IATA Packing Instructions'] = []
    info['UN Description'] = []

    un_info_list = [un_lookup.get(item, {}) for item in item_numbers]
    for u in un_info_list:
        info['UN Number'].append(u.get('UN Number', ''))
        info['IATA UN Hazard Class'].append(u.get('IATA UN Hazard Class', ''))
        info['Packing Group'].append(u.get('Packing Group', ''))
        info['IATA Packing Instructions'].append(u.get('IATA Packing Instructions', ''))
        info['UN Description'].append(u.get('UN Description', ''))

    return info

TSV_HEADERS = [
    "",
    "Ship to",
    "Job Description",
    "Shipper",
    "Consignee",
    "Airport Departure",
    "Airport Destination",
    "Airway Bill No.",
    "Shipper Reference Number",
    "Shipment Type",
    "UN or ID NO.",
    "Proper shipping name",
    "Packing Group",
    "PCS/AP Qty",
    "Type of Packing",
    "Weight",
    "Pack",
    "Label Marking",
    "OP Qty",
    "Auth",
    "User",
    "Reference Number",
    "Remarks (CS)",
    "Pickp Address",
    "Ship To Address",
    "Mode of Transport",
    "Services",
    "Service Qty",
    "Signature"
]
# Store extracted data globally (temporary cache for export)
all_extracted_info_cache = []

ship_address = """DHL Supply Chain Singapore Pte Ltd
40 Alps Avenue #03-01
Singapore 498781
"""

consignee_address = """GE Healthcare Global Parts Company Inc
C/O DHL Global Forwarding (S) Pte Ltd
40 Alps Avenue 3rd floor
Singapore 498781 SG"""

sheet2 = spreadsheet.worksheet("Sheet2")  # or the actual tab name
iata_headers = sheet2.row_values(1)
iata_headers = [h for h in iata_headers if h.strip()]
iata_headers = list(dict.fromkeys(iata_headers))

iata_data = sheet2.get_all_records(expected_headers=iata_headers)
iata_df = pd.DataFrame(iata_data)

@app.route('/download_tsv', methods=['POST'])
def download_tsv_post():
    data = request.get_json()
    if not data:
        return "⚠️ No data received.", 400

    signature = data.get("signature", "")
    user = data.get("user", "")
    checklist_deliveries = set(data.get("checklist", []))
    table_data = data.get("table_data", [])

    if not table_data:
        return "⚠️ No table data available.", 400

    rows = []
    processed_deliveries = set()
    first_dn = True

    for info in table_data:
        full_dn = info.get("DN NUMBER", "").strip()
        delivery_match = re.match(r"(\d+)", full_dn)
        delivery = delivery_match.group(1) if delivery_match else full_dn

        total_boxes = int(info.get("TOTAL BOXES", 0) or 0)
        weight = float(info.get("TOTAL WEIGHT", 0) or 0)
        un = str(info.get("UN NUMBER", "")).strip()
        packing_instructions = str(info.get("IATA PACKING INSTRUCTIONS", "")).upper()
        auth_value = "IB" if packing_instructions and ("II" in packing_instructions or "IB" in packing_instructions) else ""
        delivery1 = "DN# " + delivery

        # Determine Mode of Transport
        mode_of_transport = "Cargo (Air)"
        try:
            un_int = int(un)
        except:
            un_int = None
            
        if un_int == 3480 and auth_value == "IB":
            remarks_cs = "Max net 10kg. CAO, Battery Label, Handling Label"
        elif un_int == 3480 and auth_value == "":
            remarks_cs = "Max net 35kg. CAO & Battery Label"
        elif un_int == 3090 and auth_value == "":
            remarks_cs = "Max net 35kg. CAO & Battery Label"
        elif un_int == 3090 and auth_value == "IB":
            remarks_cs = "Max net 2.5kg. CAO, Battery Label, Handling Label"
        elif un_int == 1950 and auth_value == "IB":
            remarks_cs = "Max net 75kg"
        elif un_int == 1950 and auth_value == "":
            remarks_cs = "Max net 75kg"
        elif un_int == 3164 and auth_value == "":
            remarks_cs = "Max net no limit. Class 2.2 Label"
        elif un_int == 3164 and auth_value == "IB":
            remarks_cs = "Max net no limit. Class 2.2 Label"
        else:
            remarks_cs = ""

        # Look up maximum quantity for PAX safely
        iata_max_pax_qty = None
        
        try:
            # Ensure UN is numeric before converting
            un_clean = str(un).strip()
        
            if un_clean.isdigit() and 'UN_Number' in iata_df.columns and 'Maximum quantity for PAX' in iata_df.columns:
                un_int = int(un_clean)
                filtered_iata = iata_df.loc[iata_df['UN_Number'] == un_int, 'Maximum quantity for PAX']
        
                if not filtered_iata.empty:
                    iata_max_pax_qty = float(filtered_iata.iloc[0])
            else:
                logging.warning(f"Invalid or missing UN value: '{un}'")
        
        except Exception as e:
            logging.error(f"IATA lookup failed for UN='{un}': {e}")


        # Determine Mode of Transport based on comparison
        mode_of_transport = "Cargo (Air)"

        if iata_max_pax_qty is not None and weight < iata_max_pax_qty:
            print(weight)
            mode_of_transport = "PASSENGER (AIR)"
                
        if delivery not in processed_deliveries:
            # Add empty row for new DN (except for first DN)
            if not first_dn:
                empty_row = {header: "" for header in TSV_HEADERS}
                rows.append(empty_row)
            first_dn = False
            # Full row for first occurrence
            row = {
                "": "#",
                "Job Description": "GE Healthcare",
                "Shipper": consignee_address,
                "Consignee": info.get("CONSIGNEE ADDRESS", ""),
                "Airport Departure": "SINGAPORE",
                "Airport Destination": " ",
                "Airway Bill No.": " ",
                "Shipper Reference Number": delivery1,
                "Shipment Type": "Non Radioactive",
                "UN or ID NO.": un,
                "Proper shipping name": info.get("UN DESCRIPTION", ""),
                "Packing Group": info.get("PACKING GROUP", ""),
                "PCS/AP Qty": total_boxes,
                "Type of Packing": "Fibreboard Box",
                "Weight": weight,
                "Pack": "STD",
                "Label Marking": delivery,
                "OP Qty": "1",
                "Auth": auth_value,
                "User": user,
                "Reference Number": delivery1,
                "Remarks (CS)": remarks_cs,
                "Pickp Address": "-",
                "Ship To Address": ship_address,
                "Mode of Transport": mode_of_transport,
                "Services": "DG Declaration",
                "Service Qty": 1,
                "Signature": signature
            }
            rows.append(row)

            # DG Packaging row
            packaging_row = {header: "" for header in TSV_HEADERS}
            packaging_row["Services"] = "Packaging"
            packaging_row["Service Qty"] = info.get("TOTAL NUMBER OF CONTAINERS", "")
            rows.append(packaging_row)  
            

            processed_deliveries.add(delivery)
            
            # Optional: add Checklist Service
            if delivery in checklist_deliveries:
                checklist_row = {h: "" for h in TSV_HEADERS}
                checklist_row["Services"] = "Checklist service fee "
                checklist_row["Service Qty"] = 1
                rows.append(checklist_row)
            print(checklist_deliveries)
        else:
            # Duplicate DN → simplified row
            simplified_row = {header: "" for header in TSV_HEADERS}
            simplified_row.update({
                "UN or ID NO.": un,
                "Proper shipping name": info.get("UN DESCRIPTION", ""),
                "Packing Group": info.get("PACKING GROUP", ""),
                "PCS/AP Qty": total_boxes,
                "Type of Packing": "Fibreboard Box",
                "Weight": weight,
                "Pack": "STD",
                "Label Marking": delivery,
                "OP Qty": "1",
                "Auth": auth_value
            })
            rows.append(simplified_row)

    # Generate TSV
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=TSV_HEADERS, delimiter='\t', extrasaction='ignore')
    writer.writeheader()
    writer.writerows(rows)

    response = Response(output.getvalue(), mimetype="text/tab-separated-values")
    response.headers["Content-Disposition"] = "attachment; filename=shipment_data.tsv"
    return response

def load_google_sheet():
    """Reload the Google Sheet and return a DataFrame."""
    sheet = spreadsheet.sheet1
    headers = sheet.row_values(1)
    headers = [header for header in headers if header.strip()]
    headers = list(dict.fromkeys(headers))
    data = sheet.get_all_records(expected_headers=headers)
    return pd.DataFrame(data)

@app.route('/view_sheet', methods=['GET', 'POST'])
def view_sheet():
    """View and add rows to Google Sheet data."""
    # Reload the Google Sheet into a DataFrame
    df = load_google_sheet()

    # If there's no data, show a warning
    if df.empty:
        return render_template('view_sheet.html', message="⚠️ The Google Sheet is empty or headers are missing.")

    # Get headers and rows for the table
    headers = df.columns.tolist()
    rows = df.values.tolist()

    # If POST request, add a new row to the Google Sheet
    if request.method == 'POST':
        # Create a dictionary from the form data
        new_row = {header: request.form.get(header, '') for header in headers}

        # Append the new row to the sheet (using gspread)
        sheet = spreadsheet.sheet1
        sheet.append_row([new_row.get(header, '') for header in headers])

        # Reload the Google Sheet after adding the row
        df = load_google_sheet()
        rows = df.values.tolist()
        return render_template('view_sheet.html', message="✅ Row added successfully!", headers=headers, rows=rows)

    # Render the sheet data with the form
    return render_template('view_sheet.html', headers=headers, rows=rows)

# Flask route for processing PDF and displaying extracted data
@app.route('/', methods=['GET', 'POST'])
def index():
    global all_extracted_info_cache
    all_extracted_info = []

    if request.method == 'POST':
        signature = request.form.get('signature', '').strip()
        app.config['USER_SIGNATURE'] = signature  # store globally
        user = request.form.get('user', '').strip()
        app.config['USER_DHL'] = user  # store globally

        if 'pdf_files' in request.files:
            pdf_files = request.files.getlist('pdf_files')
            for pdf_file in pdf_files:
                if pdf_file.filename.endswith('.pdf'):
                    try:
                        reader = PyPDF2.PdfReader(pdf_file)
                        full_text = ""
                        for page in reader.pages:
                            text = page.extract_text()
                            if text:
                                full_text += text + "\n"

                        if not full_text.strip():
                            info = {
                                'Filename': pdf_file.filename,
                                'Error': 'No readable text found in PDF.'
                            }
                        else:
                            info = extract_info(full_text)
                            info['Filename'] = pdf_file.filename
                            if not any(v for k, v in info.items() if k != 'Filename' and v):
                                info['Warning'] = 'No extractable information found.'
                    except Exception as e:
                        info = {
                            'Filename': pdf_file.filename,
                            'Error': f'Failed to process PDF: {str(e)}'
                        }

                    all_extracted_info.append(info)

    # ✅ Cache for TSV export
    all_extracted_info_cache = all_extracted_info

    return render_template('index.html', all_extracted_info=all_extracted_info)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5151, debug=True)






