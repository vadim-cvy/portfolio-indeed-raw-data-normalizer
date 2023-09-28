# Notes

1. Download data from: [https://data.world/search?entryTypeLabel=dataset&q=indeed&type=resources](https://data.world/search?entryTypeLabel=dataset&q=indeed&type=resources).
2. Normalize data (file sizes are reduced about 2-2.5 times)
    * Remove useless data
    * Remove plain text description (leave html only)
    * use csv format instead of json
3. Further data preparation and normalization will take place in mysql.