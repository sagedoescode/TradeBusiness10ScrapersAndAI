import os
import csv
import shutil
import asyncio
from datetime import datetime
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM, pipeline
import os
from huggingface_hub import hf_hub_download


async def get_nationality(full_name, text_generation_pipeline):
    prompt = f"What is the most likely heritage or ethnic background for the name '{full_name}'? Respond with a single word like Italian, Spanish, Portuguese, etc."
    result = text_generation_pipeline(prompt, max_length=30)
    heritage = result[0]['generated_text'].strip().split()[0]  # Take only the first word

    # List of accepted heritages
    accepted_heritages = [
        # Europe
        "British", "French", "German", "Italian", "Spanish", "Portuguese", "Dutch",
        "Belgian", "Swiss", "Austrian", "Greek", "Swedish", "Norwegian", "Finnish",
        "Danish", "Polish", "Russian", "Ukrainian", "Romanian", "Hungarian", "Czech",
        "Bulgarian", "Serbian", "Croatian", "Irish",

        # Asia
        "Chinese", "Japanese", "Korean", "Indian", "Pakistani", "Bangladeshi",
        "Vietnamese", "Thai", "Indonesian", "Filipino", "Malaysian", "Singaporean",

        # Middle East
        "Turkish", "Iranian", "Saudi", "Israeli", "Lebanese", "Syrian", "Iraqi",
        "Emirati", "Egyptian",

        # Africa
        "Nigerian", "South African", "Kenyan", "Ethiopian", "Ghanaian", "Moroccan",
        "Algerian", "Tunisian", "Senegalese",

        # Americas
        "American", "Canadian", "Mexican", "Brazilian", "Argentine", "Colombian",
        "Peruvian", "Chilean", "Venezuelan",

        # Oceania
        "Australian", "New Zealander",

        # Broader categories
        "African", "European", "Asian", "Middle Eastern", "Latin American",
        "Caribbean", "Pacific Islander"
    ]

    if heritage in accepted_heritages:
        return heritage
    else:
        return heritage

async def process_row(row, writer, text_generation_pipeline):
    if not isinstance(row, dict):
        print(f"Unexpected row type: {type(row)}. Row content: {row}")
        return

    owner_first_name = row.get('Owner First Name', '').strip() if row.get('Owner First Name') else ''
    owner_last_name = row.get('Owner Last Name', '').strip() if row.get('Owner Last Name') else ''
    full_name = f"{owner_first_name} {owner_last_name}".strip()

    assumed_heritage = row.get("Language", "").strip() if row.get("Language") else ""

    if full_name and assumed_heritage == "":
        print(f"Processing: {full_name}")
        try:
            heritage = await get_nationality(full_name, text_generation_pipeline)
            row["Language"] = heritage
            print(f"Name: {full_name}, Heritage: {heritage}")
        except Exception as e:
            print(f"An unexpected error occurred: {str(e)}")
            row["Language"] = ""
    else:
        row["Language"] = assumed_heritage

    writer.writerow(row)

async def process_csv_heritage(state, niche):
    HUGGING_FACE_API_KEY = os.environ.get("HUGGING_FACE_API_KEY")
    # Define model information
    model_id = "google/flan-t5-large"
    # filenames = [
    #     "config.json",
    #     "flax_model.msgpack",
    #     "generation_config.json",
    #     "model.safetensors",
    #     "pytorch_model.bin",
    #     "special_tokens_map.json",
    #     "spiece.model",
    #     "tf_model.h5",
    #     "tokenizer.json",
    #     "tokenizer_config.json"
    # ]
    # for filename in filenames:
    #     downloaded_model_path = hf_hub_download(
    #         repo_id=model_id,
    #         filename=filename,
    #         token=HUGGING_FACE_API_KEY
    #     )
    #     print(downloaded_model_path)
    # Set up the model and pipeline
    tokenizer = AutoTokenizer.from_pretrained(model_id, legacy=False)
    model = AutoModelForSeq2SeqLM.from_pretrained(model_id)
    text_generation_pipeline = pipeline("text2text-generation", model=model, device=-1, tokenizer=tokenizer,
                                        max_length=10)

    input_file = fr"C:\Users\Sage\PycharmProjects\MasterScraper\data\{state}_{niche}_central_stage_3.csv"
    output_final_file = fr"C:\Users\Sage\PycharmProjects\MasterScraper\data\updated_{state}_{niche}_businesses_with_heritage.csv"
    backup_file = fr"C:\Users\Sage\PycharmProjects\MasterScraper\data\backup_{state}_{niche}_businesses_with_heritage.csv"

    shutil.copy2(input_file, backup_file)

    try:
        with open(input_file, 'r', newline='', encoding='utf-8') as infile, \
                open(output_final_file, 'w', newline='', encoding='utf-8') as outfile:

            reader = csv.DictReader(infile)
            fieldnames = reader.fieldnames
            print("Fieldnames:", fieldnames)
            if "Language" not in fieldnames:
                fieldnames.append("Language")

            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            tasks = []
            for row in reader:
                task = asyncio.create_task(process_row(row, writer, text_generation_pipeline))
                tasks.append(task)

            await asyncio.gather(*tasks)


        print("Processing completed successfully. Check the output file for results.")
    except Exception as e:
        print(f"An error occurred during processing: {str(e)}")
        print("Restoring the original file from backup...")
        os.replace(backup_file, output_final_file)
        print("Original file restored. No changes were made.")
    finally:

        if os.path.exists(backup_file):
            os.remove(backup_file)

    print("Processing completed. Check the output file for results.")

async def get_heritage_local(state, niche):

    await process_csv_heritage(state=state, niche=niche)

if __name__ == "__main__":
    asyncio.run(get_heritage_local(state="massachusetts", niche="Counter Top"))