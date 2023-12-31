# Prepare and import jobs dataset to DB

## Filter and normalize the dataset

`raw-data-to-csv.js` script gets multiple `jsonld` dataset files of Indeed archive job postings (located in `/dataset/raw/` dir) as an input and outputs a single normalized csv document (`/dataset/normalized/normalized.csv`) which can be imported into DB.

Features:
1. Removes useless data such as dataset related fields and unneded job fields (apply URL, etc).
2. Removes plain text description (leaves html only).
3. Normalizes json structure between different input documents (one document may miss the fields presented in another one).
4. Normalizes some column values to make the output file size smaller.
5. Filters duplicate jobs between different input documents.

### Input data

Datasets:
* USA 2020.10.01 - 2020.12.31 (~30K postings)
* USA 2021.01.01 - 2021.03.31 (~30K postings)
* USA 2021.07.01 - 2021.09.30 (~30K postings)
* India 2021.04.01 - 2021.06.30 (~30K postings)

Total input files size: ~1 GB.

Source: [https://data.world/search?entryTypeLabel=dataset&q=indeed&type=resources](https://data.world/search?entryTypeLabel=dataset&q=indeed&type=resources)

### Result

Total jobs: 119,895.

Output file size: ~500 MB.

## Ipmort the data