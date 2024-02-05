# Live Class Luigi Pacmann

- Repository untuk Live Class Week 7 course Intro to Data Engineer Pacmann
- Sudah disediakan `run_etl.sh` untuk membuat scheduler dengan menggunakan Cron, kalian bisa menyesuaikan dengan lokal masing - masings
- Karena di course ini kita sering menggunakan credentials database seperti username, password, hostname, dan database name disarankan untuk membuat `.env` file

```Dotenv
# .env file example
DB_USERNAME="budiono"
DB_PASSWORD="budi_kapal_lawd"
```

- Alasannya agar lebih secured dan tidak terekspos di public code dan repository. Untuk tutorialnya, bisa merujuk ke artikel berikut https://dev.to/emma_donery/python-dotenv-keep-your-secrets-safe-4ocn

### Data Source
---

- Hotel Data: https://hub.docker.com/r/shandytp/hotel-booking-docker-db 
- Anime Data: https://myanimelist.net/anime/season/2024/winter
- Manga Data: https://api.jikan.moe/v4/manga/{id}/full

### Luigi Pipeline Output
---

![luigi_pipeline.png](assets/luigi_pipeline.png)
