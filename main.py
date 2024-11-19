from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing
import os
from pypdf import PdfReader
import re
import requests
import time

# Text style functions
def red(text):
    return f"\033[31m{text}\033[0m"

def green(text):
    return f"\033[32m{text}\033[0m"

def bold(text):
    return f"\033[1m{text}\033[0m"


# Gets & returns text from the given file
def get_file_contents(file_path):
    with open(file_path, 'r') as file:
        return file.read()


# Writes input text to the given csv file by individual row
def edit_csv(csv_directory, data_name, data, edit_type='w'):
    if edit_type not in ['w', 'a']:
        raise ValueError('Invalid edit_type, must be "w" or "a"')
    with open(f"{csv_directory}/{data_name}.csv", edit_type) as csv:
        csv.write(data + '\n')


# Clears the given directory of all contents
def clear_directory(directory):
    for file in os.listdir(directory):
        os.remove(f"{directory}/{file}")

# Normalizes all community names into their URL format
def data_normalize(text, url_normalize=True, csv_normalize=False):
    data_ascii = ''.join(filter(lambda char: char.isascii(), text))
    data_normalized_url = re.findall(
        r'.+', re.sub(r'[ \/.]+', "-",
            data_ascii.lower())) if url_normalize else None
    data_normalized_csv = re.findall(r'.+',data_ascii.upper()) if csv_normalize else None

    return data_normalized_url or data_normalized_csv or data_ascii


# Fetches the PDF file from a given URL
def fetch_pdf_and_append(output_directory, csv_directory, community_pdf, community_csv, data_name, pdf_batch_logs, csv_creation_logs):
    pdf_path = f"{output_directory}/{community_pdf}.pdf"
    if not os.path.exists(pdf_path):
        try:
            response = requests.get(
                f"https://www.calgary.ca/content/dam/www/csps/cns/documents/community_social_statistics/community-profiles/{community_pdf}.pdf"
            )
            response.raise_for_status()
            with open(pdf_path, "wb") as pdf:
                pdf.write(response.content)
            if pdf_batch_logs:
                print(f"Successfully fetched {green(community_pdf+'.pdf')}")
                
        except requests.HTTPError as e:
            if pdf_batch_logs:
                print(red(f"Error fetching {community_pdf}.pdf: {e}"))
                pass
                
    # Proceed with appending to CSV once PDF is fetched
    immigrants, non_immigrants = extract_immigration_data(get_pdf_page(output_directory, community_pdf, 8))
    
    if csv_creation_logs:
        if immigrants == 'N/A' and non_immigrants == 'N/A':
            print(f"No 'Immigrants' or 'Non-Immigrants' data found in {red(community_csv.capitalize())}")
        elif immigrants == "N/A":
            print(f"No 'Immigrants' data found in {red(community_csv.title())}")
        elif non_immigrants == "N/A":
            print(f"No 'Non-Immigrants' data found in {red(community_csv.title())}")
        else:
            print(f"Successfully appended {green(community_csv.title())}")

    if immigrants and non_immigrants:
        data = f"{community_csv},{immigrants},{non_immigrants}"
        with open(f"{csv_directory}/{data_name}.csv", 'a') as csv:
            csv.write(data + '\n')


def get_pdf_page(output_directory, community, page):
    reader = PdfReader(f'{output_directory}/{community}.pdf')
    return reader.pages[page - 1].extract_text()

def extract_immigration_data(text):
    if text == "name":
        return ""
    IMMIGRANTS = r'(?<=Immigrants )[\d,]+'
    NON_IMMIGRANTS = r'(?<=immigrants )[\d,]+'
    try:
        immigrants = re.search(IMMIGRANTS, text)
        non_immigrants = re.search(NON_IMMIGRANTS, text)
        
        immigrants = immigrants.group().replace(',', '') if immigrants else 'N/A'
        non_immigrants = non_immigrants.group().replace(',', '') if non_immigrants else 'N/A'
        
    except AttributeError:
        immigrants = non_immigrants = 'N/A'  # Handle missing data
    return [immigrants, non_immigrants]

# def extract_population


def finalize_data_concurrent(clear_pdfs=False, pdf_batch_logs=True, csv_creation_logs=True):
    start_time = time.time()  # Start the compilation timer

    pdf_directory = 'pdf_files'
    csv_directory = 'csv_files'
    community_names_directory = 'community-names.txt'
    data_name = 'calgary-immigrants-by-community'
    
    communities_pdf = data_normalize(
        get_file_contents(community_names_directory), True, False)
    communities_csv = data_normalize(
        get_file_contents(community_names_directory), False, True)
    
    edit_csv(csv_directory, data_name, 'Community,Immigrants,Non-Immigrants', 'w')

    num_communities = len(communities_pdf)
    num_cores = multiprocessing.cpu_count()
    max_workers = min(num_cores // 2, num_communities, 10)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                fetch_pdf_and_append,
                pdf_directory,
                csv_directory,
                community_pdf,
                community_csv,
                data_name,
                pdf_batch_logs,
                csv_creation_logs
            ): (community_pdf, community_csv)
            for community_pdf, community_csv in zip(communities_pdf, communities_csv)
        }

        for future in as_completed(futures):
            pass  # Future results can be handled here if needed

    if clear_pdfs:
        clear_directory(pdf_directory)

    end_time = time.time()  # End the compilation timer
    elapsed_time = end_time - start_time
    print(bold(f"Compilation completed in {elapsed_time:.2f} seconds"))

def main(logs=True):
    if not os.path.exists('csv_files'):
        os.makedirs('csv_files')
    if not os.path.exists('pdf_files'):
        os.makedirs('pdf_files')
    choice = ""
    while choice != "5":
        print("1. Compile data & clear PDFs")
        print("2. Compile data & keep PDFs")
        print("3. Clear PDFs")
        print("4. Clear CSVs")
        print("5. Exit")
        choice = input("Enter your choice (1-5): ")
        if choice not in set("12345"):
            print("Invalid choice. Please enter a number from 1 to 5.")
            time.sleep(1)
            os.system('clear')
        elif choice == "1":
            finalize_data_concurrent(clear_pdfs=True, pdf_batch_logs=logs, csv_creation_logs=logs)
        elif choice == "2":
            finalize_data_concurrent(clear_pdfs=False, pdf_batch_logs=logs, csv_creation_logs=logs)
        elif choice == "3":
            clear_directory('pdf_files')
            print(bold("PDFs cleared."))
            time.sleep(1)
            os.system('clear')
        elif choice == "4":
            clear_directory('csv_files')
            print(bold("CSVs cleared."))
            time.sleep(1)
            os.system('clear')
        else:
            print("Exiting...")
            break

main(logs=True)