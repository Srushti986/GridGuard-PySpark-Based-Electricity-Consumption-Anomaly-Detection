# Google Colab setup guide

1. Upload `GridGuard_Colab.ipynb` to https://colab.research.google.com (File ▸ Upload notebook) or open it from Google Drive.
2. Check the **Configuration** cell. Defaults: `USE_HDFS = False` (Mode B, standalone). Change thresholds if you wish.
3. **Runtime ▸ Run all.** Cells install Java (if missing) and PySpark, download the UCI dataset, run the pipeline, plot, and offer a ZIP download.
4. If the dataset download fails, upload `household_power_consumption.txt` via the Files panel into `/content/gridguard_data/` and re-run that cell.
5. Read the coloured banner in section 4: it states the storage mode that is *actually* active.

## Mode A – real HDFS from Colab
Colab runs in Google's cloud, so `localhost` is the Colab VM. A Docker cluster on your laptop is **not reachable**. You need a NameNode with a public/VPN-reachable address:
* set `USE_HDFS = True` and `HDFS_NAMENODE_URI = "hdfs://<host>:8020"`;
* open NameNode port 8020 and DataNode port 9866; DataNodes must advertise a hostname Colab can resolve;
* this dev setup has no authentication – do not expose it publicly in real use.
If the NameNode cannot be reached the notebook falls back to Mode B and says so.

## View results in the dashboard
Unzip the downloaded `gridguard_outputs.zip` into the project's `outputs/` folder, then `streamlit run app.py`.
