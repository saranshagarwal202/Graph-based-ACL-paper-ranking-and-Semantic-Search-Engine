import json
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
import sys

MODEL_NAME = 'all-MiniLM-L6-v2'

def create_and_save_embeddings(input_path, output_path):
    """
    Loads papers from a JSON file, generates sentence embeddings for their abstracts,
    and saves the augmented data to a new JSON file.
    """
    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            papers = json.load(f).get("papers", [])
    except FileNotFoundError:
        print(f"Error: Input file not found at {input_path}")
        return
    
    if not papers:
        print("Error: No papers found in the input file.")
        return

    print(f"Initializing sentence transformer model: {MODEL_NAME}")

    model = SentenceTransformer(MODEL_NAME)

    abstracts = [
        paper.get('abstract') or paper.get('title', '')
        for paper in papers
    ]

    print(f"Generating embeddings for {len(abstracts)} abstracts")
    

    embeddings = model.encode(
        abstracts,
        show_progress_bar=True,
        normalize_embeddings=True
    )

    print("Embeddings generated successfully.")
    
    for i, paper in enumerate(papers):
        paper['abstract_embedding'] = embeddings[i].tolist()

    output_data = {"papers": papers}

    print(f"Saving augmented data with embeddings to: {output_path}")
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2)


if __name__ == "__main__":
    input_file = "data/processed/papers.json"
    output_file = "data/processed/papers_with_embeddings.json"
    create_and_save_embeddings(input_file, output_file)