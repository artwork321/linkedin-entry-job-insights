import json
import pandas as pd
import spacy
from spacy.tokens import DocBin
from sklearn.model_selection import train_test_split

# this function will convert to the necessary json format spacy requires to prepare the data
def convert_spacy_format(data):
    training_data = []

    for row in data:
        temp_dict = {}
        temp_dict["text"] = row["document"]
        temp_dict["entities"] = []

        for anno in row["annotation"]:
            start = anno["start"]
            end = anno["end"]
            label = anno["label"].upper()
            temp_dict["entities"].append((start, end, label))

        training_data.append(temp_dict)

    return training_data

def convert_docbin(training_data):
    nlp = spacy.blank("en")
    doc_bin = DocBin()

    for training_ins in training_data:
        text = training_ins["text"]
        entities = training_ins["entities"]
        doc = nlp.make_doc(text)
        ents = []

        for start, end, label in entities:
            span = doc.char_span(start, end, label=label, alignment_mode="contract")
            if span is None:
                print(f"Skipping entity [{label}] in text [{text}] due to misalignment.")
            else:
                ents.append(span)
        
        doc.ents = ents
        doc_bin.add(doc)

    return doc_bin


def prepare_ner_data(input_json_path, output_dir="data_files/ner_data", train_size=0.8):
    # Load the annotated data from JSON file
    with open(input_json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Convert to spaCy format
    spacy_data = convert_spacy_format(data)

    # Split the data into training and testing sets
    train_data, test_data = train_test_split(spacy_data, train_size=train_size, random_state=42)

    # Convert to DocBin format
    train_docbin = convert_docbin(train_data)
    test_docbin = convert_docbin(test_data)

    # Save the DocBin files
    train_output_path = f"{output_dir}/train.spacy"
    test_output_path = f"{output_dir}/test.spacy"

    train_docbin.to_disk(train_output_path)
    test_docbin.to_disk(test_output_path)

    print(f"Training data saved to {train_output_path}")
    print(f"Testing data saved to {test_output_path}")