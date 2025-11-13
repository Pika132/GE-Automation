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

app = Flask(__name__)

scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]


credentials_json = json.loads(os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON"))
credentials = service_account.Credentials.from_service_account_info(credentials_json, scopes=scope)
client = gspread.authorize(credentials)

# Open the Google Sheet using the sheet name or the sheet key (if you have the sheet's URL)
spreadsheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1THXb-qxNYQQ-13UuDxKUKM168qn7TqvkyDemh9hcbiI/edit?gid=0#gid=0")
sheet = spreadsheet.sheet1  # You can also specify the sheet name instead of .sheet1

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

# Extract the information from the text
def extract_info(text):
    info = {}

    # --- Delivery number ---
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
    
@app.route('/download_tsv')
def download_tsv():
    global all_extracted_info_cache

    signature = request.args.get('signature', '').strip()
    checklist_param = request.args.get('checklist', '[]')
    try:
        import json
        checklist_deliveries = set(json.loads(checklist_param))
    except Exception:
        checklist_deliveries = set()

    if not all_extracted_info_cache:
        return "⚠️ No data available. Please upload PDFs first.", 400

    rows = []
    processed_deliveries = set()

    for info in all_extracted_info_cache:
        delivery = info.get("Delivery", "")

        first_occurrence = delivery not in processed_deliveries

        # Add all boxes for this delivery
        for i, b in enumerate(info.get("Boxes", [])):
            row = {
                "Job Description": "GE Healthcare",
                "Shipper": consignee_address,
                "Consignee": info.get("Ship To", ""),
                "Shipper Reference Number": delivery,
                "Shipment Type": "Non Radioactive",
                "UN or ID NO.": (info.get("UN Number") or [""])[0],
                "Proper shipping name": (info.get("UN Description") or [""])[0],
                "Packing Group": (info.get("Packing Group") or [""])[0],
                "PCS/AP Qty": b.get("Total Boxes", ""),
                "Type of Packing": "Fibreboard Box",
                "Weight": b.get("Weight", ""),
                "Pack": "OP",
                "Label Marking": delivery,
                "OP Qty": "1",
                "Auth": "IB",
                "User": "",
                "Reference Number": delivery,
                "Remarks (CS)": "Max net 10kg. CAO, Battery Label, Handling Label",
                "Pickp Address": "-",
                "Ship To Address": ship_address,
                "Mode of Transport": "Cargo (Air)",
                # Only add DG Declaration for the first box of first occurrence
                "Services": "DG Declaration" if first_occurrence and i == 0 else "",
                "Service Qty": 1 if first_occurrence and i == 0 else "",
                "Signature": signature
            }
            rows.append(row)

        # Add DG Packaging row only once per delivery, with all other fields empty
        if first_occurrence:
            packaging_row = {header: "" for header in TSV_HEADERS}
            packaging_row["Services"] = "DG Packaging"
            packaging_row["Service Qty"] = info.get("Total Containers", "")
            rows.append(packaging_row)

        # ✅ Add Checklist Service if delivery is selected
        if delivery in checklist_deliveries:
            checklist_row = {header: "" for header in TSV_HEADERS}
            checklist_row["Services"] = "Checklist Service"
            checklist_row["Service Qty"] = 1
            rows.append(checklist_row)

        processed_deliveries.add(delivery)

        # Optional: separate deliveries visually
        rows.append({header: "" for header in TSV_HEADERS})

    # Create TSV in memory
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=TSV_HEADERS, delimiter='\t', extrasaction='ignore')
    writer.writeheader()
    writer.writerows(rows)

    response = Response(output.getvalue(), mimetype="text/tab-separated-values")
    response.headers["Content-Disposition"] = "attachment; filename=shipment_data.tsv"
    return response


# Flask route for processing PDF and displaying extracted data
@app.route('/', methods=['GET', 'POST'])
def index():
    global all_extracted_info_cache
    all_extracted_info = []

    if request.method == 'POST':
        signature = request.form.get('signature', '').strip()
        app.config['USER_SIGNATURE'] = signature  # store globally

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







