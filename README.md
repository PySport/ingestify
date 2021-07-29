# ingestify

```yaml
config:
  collection_db_url: ${DATABASE_URL}/collection
  
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
    configuration:
      season_id: 42
        competition_id: [9, 78, 10]
      type: ["lineup", "events"]
    target: 
      sink: database
      table: statsbomb_lineup


        
```
