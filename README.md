# tap-tiktok

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [TikTok Marketing Api](https://ads.tiktok.com/marketing_api/docs)
- Extracts the following Reports:
  - ad_id_reports
  - adgroup_id_reports
  - campaign_id_reports
  - advertiser_id_reports
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Quick Start

1. Install

    pip install git+https://github.com/SageData-OOD/tap-tiktok

2. Create the config file

   Create a JSON file called `config.json`. Its contents should look like:

   ```json
    {
        "start_date": "2021-09-20",
        "advertiser_id": "xxxxxxxxxxxxxxxxxxx",
        "report_type": "BASIC",
        "token": "<access token after OAuth process>",
        "disable_collection": true
    }
    ```

   - The `start_date`: Query start date. Format should be YYYY-MM-DD
   - The `advertiser_id` : Tiktok Advertiser Id
   - The `report_type`: Currently Supporting [BASIC]
   - The `token`: Access token after OAuth process

4. Run the Tap in Discovery Mode

    tap-tiktok -c config.json -d

   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode

    tap-tiktok -c config.json --catalog catalog-file.json

---

Copyright &copy; 2021 SageData
