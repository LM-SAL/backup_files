import os
from pathlib import Path

import drms


def get_and_save_pointing_table(save_path: Path):
    """
    Get and save the AIA pointing table.

    Parameters
    ----------
    save_path : Path
        The full file path where the pointing table CSV file will be saved.
    """
    try:
        jsoc_result = drms.Client().query("aia.response[][!1=1!]", key="**ALL**")
    except Exception as e:
        msg = f"Unable to query the JSOC.\n Error message: {e}"
        raise OSError(msg) from e
    if len(jsoc_result) == 0:
        msg = "No data found for this query: aia.response[][!1=1!], key: **ALL**"
        raise RuntimeError(msg)
    save_path.parent.mkdir(parents=True, exist_ok=True)
    jsoc_result.to_csv(save_path, index=False)


if __name__ == "__main__":
    # I want this to error if the environment variable is not set
    get_and_save_pointing_table(Path(os.environ["OUTPUT_DIR"]) / "aia_pointing_table.csv")
