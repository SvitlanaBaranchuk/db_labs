1. Use 'docker-compose build && docker-compose up -d --force-recreate' to open database
2. Uncomment db_data(), fill_year_column(), combine_data() and comment main() to fill a database with data
3. Wait
4. Run flyway part (from docker-compose, for example) to migrate data
5. Wait more (x2 or x3 of time from step 3)
6. Comment all except main(), uncomment it
7. Run main.py (from docker-compose, for example)