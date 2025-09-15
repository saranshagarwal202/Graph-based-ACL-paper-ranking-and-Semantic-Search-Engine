import sys
import json
from sentence_transformers import SentenceTransformer

MODEL_NAME = 'all-MiniLM-L6-v2'

def embed_query():
    """
    Reads a query from a command-line argument, generates its embedding,
    and prints it to stdout as a JSON array.
    """

    if len(sys.argv) < 2:
        print("Error: No query provided. Please pass the query as an argument.", file=sys.stderr)
        sys.exit(1)

    query = sys.argv[1]
    
    model = SentenceTransformer(MODEL_NAME)
    
    embedding = model.encode(query, normalize_embeddings=True)
    
    print(json.dumps(embedding.tolist()))

if __name__ == "__main__":
    embed_query()