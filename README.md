# Customer Master Data Migration - Coding Assignment

## Contents

- `Customer_Transformation.ipynb` - executed Jupyter notebook with documentation, profiling, DQ checks, transformations and validation.
- `transform.py` - reusable transformation script.
- `CustomerLoad.csv` - final SAP load flat file.
- `data_quality_report.csv` - traceable DQ results.
- `data/CustomerExtract.csv` - supplied source extract.
- `data/CustomerSpec.xlsx` - supplied transformation specification.
- `requirements.txt` - Python dependencies.

## Run

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate

pip install -r requirements.txt
python transform.py
```

To inspect/run interactively:

```bash
jupyter notebook Customer_Transformation.ipynb
```

## Result

- Source records: 100
- Source columns: 183
- Target fields marked `Field Used = Y`: 50
- Output records: 100
- Output columns: 50

## Key DQ findings

The notebook surfaces the following review items:

1. 33 source columns contain data although they are not flagged as `Field Utilized in LEGACY System = Y`.
2. 90 `NAME1` values contain punctuation/special characters; these are cleaned before load.
3. The source `SPRAS` value is not `EN`; the target is set to `EN` per the specification.
4. 42 `LIFNR` values are populated, but Vendor mapping is explicitly pending a business decision, so these values are not loaded.
5. `AUFSD`, `KATR6`, `KATR10`, `KDKG1`, and `KDKG2` contain source data despite not being flagged as legacy-utilized. `AUFSD` is copied because its transformation rule explicitly says Copy Existing; the remaining fields are left blank because no approved legacy mapping is specified.
6. No duplicate `KUNNR` values were found.
7. No target field length violations were found.

## Transformation rules applied

- `BUKRS` = `G100`
- `VKORG` = `G100`
- `VTWEG` = `20`
- `SPART` = `10`
- Direct-copy fields follow the specification.
- `NAME1` is cleaned for punctuation/special characters.
- `SPRAS` = `EN`
- `ERDAT` and `ERNAM` are blank because they are system-defined.
- `LIFNR` is blank pending business approval.
- Unmapped new-system fields are not populated from unexpected source data.

The final load file is intentionally accompanied by the DQ report so that unresolved business/specification items remain visible to the migration team.
