import argparse
import csv
import os
import tempfile

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


def analyze_file(input_path):
    analyzer = SentimentIntensityAnalyzer()

    with open(input_path, newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])

        required_column = "Comment Text"
        if required_column not in fieldnames:
            raise ValueError(f"Missing required column: {required_column}")

    output_fields = list(fieldnames)
    if "vader_score" not in output_fields:
        output_fields.append("vader_score")

    for row in rows:
        comment_text = (row.get("Comment Text") or "").strip()
        scores = analyzer.polarity_scores(comment_text)
        row["vader_score"] = f'{scores["compound"]:.4f}'

    input_dir = os.path.dirname(os.path.abspath(input_path)) or "."
    with tempfile.NamedTemporaryFile(
        "w",
        newline="",
        encoding="utf-8",
        delete=False,
        dir=input_dir,
    ) as outfile:
        temp_path = outfile.name
        writer = csv.DictWriter(outfile, fieldnames=output_fields)
        writer.writeheader()
        writer.writerows(rows)

    os.replace(temp_path, input_path)
    print(f"Updated {len(rows)} comments in: {input_path}")
    print("Added/updated column: vader_score")


def main():
    parser = argparse.ArgumentParser(
        description="Run VADER sentiment analysis on Reddit housing comments."
    )
    parser.add_argument(
        "input_csv",
        help="Path to the raw CSV file with a 'Comment Text' column.",
    )
    args = parser.parse_args()

    analyze_file(args.input_csv)


if __name__ == "__main__":
    main()
