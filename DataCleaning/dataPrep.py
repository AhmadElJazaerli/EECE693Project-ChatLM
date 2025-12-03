import shutil
from pathlib import Path
import pandas as pd
from PIL import Image

# ---------------------------------------------------------
# CONFIG PATHS
# ---------------------------------------------------------
ROOT = Path(r"C:\Users\yurig\OneDrive\Desktop\EECE693Project-ChatLM\Chart-to-text\Chart-to-text\statista_dataset\dataset")
META = ROOT / "metadata.csv"

SPLITS = {
    "train": ROOT / "dataset_split" / "train_index_mapping.csv",
    "val":   ROOT / "dataset_split" / "val_index_mapping.csv",
    "test":  ROOT / "dataset_split" / "test_index_mapping.csv",
}

OUT = Path(r"C:\Users\yurig\OneDrive\Desktop\EECE693Project-ChatLM\final_dataset")
OUT.mkdir(exist_ok=True)

# ---------------------------------------------------------
# HELPERS
# ---------------------------------------------------------
def extract_id_and_type(file_no: str):
    """
    Converts:
       'two_col-21156.txt'  →  ('two_col', '21156')
       'multi_col-1088.txt' →  ('multi_col', '1088')
    """
    stem = Path(file_no).stem
    ctype, cid = stem.split("-")
    return ctype, cid


def img_is_valid(path: Path):
    try:
        with Image.open(path) as im:
            im.verify()
        return True
    except:
        return False


# ---------------------------------------------------------
# LOAD METADATA
# ---------------------------------------------------------
meta = pd.read_csv(META)
meta["id"] = meta["id"].astype(str)
meta["chartType"] = meta["chartType"].astype(str).str.lower()

valid_types = ["line", "bar", "column", "table", "pie"]

# ---------------------------------------------------------
# PROCESS EACH SPLIT
# ---------------------------------------------------------
total_copied = 0

for split_name, split_path in SPLITS.items():

    print(f"\n=== Processing {split_name.upper()} split ===")

    df_split = pd.read_csv(split_path)
    df_split.columns = [c.lower() for c in df_split.columns]

    ids, raw_types = [], []

    for file_no in df_split["file_no"]:
        raw_chart_type, chart_id = extract_id_and_type(file_no)
        ids.append(chart_id)
        raw_types.append(raw_chart_type)

    df_split = pd.DataFrame({"id": ids, "raw_type": raw_types})
    df_split = df_split.merge(meta, on="id", how="left")

    # -----------------------------------------------------
    # CREATE OUTPUT DIRS
    # -----------------------------------------------------
    for ct in valid_types:
        (OUT / split_name / ct).mkdir(parents=True, exist_ok=True)

    # -----------------------------------------------------
    # COPY FILES
    # -----------------------------------------------------
    copied = 0

    for _, row in df_split.iterrows():

        cid = row["id"]
        chart_type = row["chartType"]

        if chart_type not in valid_types:
            continue

        # Determine actual image/data location
        if row["raw_type"] == "two_col":
            img_path = ROOT / "imgs" / f"{cid}.png"
            data_txt = ROOT / "data" / f"{cid}.txt"
            data_csv = ROOT / "data" / f"{cid}.csv"
        else:  # multi_col
            img_path = ROOT / "multiColumn" / "imgs" / f"{cid}.png"
            data_txt = ROOT / "multiColumn" / "data" / f"{cid}.txt"
            data_csv = ROOT / "multiColumn" / "data" / f"{cid}.csv"

        # Skip missing or unreadable images
        if not img_path.exists() or not img_is_valid(img_path):
            continue

        # Output folder
        out_dir = OUT / split_name / chart_type
        out_dir.mkdir(exist_ok=True)

        # COPY IMAGE
        shutil.copy(str(img_path), str(out_dir / f"{cid}.png"))

        # COPY DATA FILE
        if data_txt.exists():
            shutil.copy(str(data_txt), str(out_dir / f"{cid}.txt"))
        elif data_csv.exists():
            shutil.copy(str(data_csv), str(out_dir / f"{cid}.csv"))

        copied += 1
        total_copied += 1

    print(f"Copied {copied} samples into {split_name}/")

print("\n=================================================")
print(f"DONE. Total samples copied: {total_copied}")
print("Final dataset at:", OUT)
print("=================================================\n")
