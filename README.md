# ingestify

```yaml
main:
  dataset_url: !ENV ${DATABASE_URL}
  file_url: file:///tmp/blaat2
  
sources:
  wyscout:
    type: WyScoutScraper
    credentials:
      username: ${WYSCOUT_USERNAME}
      password: ${WYSCOUT_PASSWORD}
  statsbomb:
    type: StatsbombGithubSource
  
sinks:
  database:
    url: ${DATABASE_URL}/
  
tasks:
  - source: statsbomb
    selectors:
      season_id: 42
        competition_id: [9, 78, 10]
    target: 
      type: database
      table: statsbomb_lineup


        
```
