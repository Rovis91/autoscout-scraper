# AutoScout24 Car Scraper

A production-ready Python scraper for extracting car listings from AutoScout24 and storing them in a Supabase database with user preference matching and Telegram notifications.

## Features

- **Automated Scraping**: Scrapes car listings from AutoScout24 with intelligent pagination
- **Database Integration**: Stores listings in Supabase with proper data validation and formatting
- **User Preference Matching**: Automatically links new listings to users based on their preferences
- **Telegram Notifications**: Sends completion reports and error notifications via Telegram
- **Robust Error Handling**: Comprehensive error handling with fallback strategies
- **Production Logging**: Structured logging for monitoring and debugging
- **Rate Limiting**: Respectful scraping with configurable delays

## Architecture

The scraper follows a modular architecture:

```
src/autoscout/
├── scraper.py          # Main scraping logic
├── data_processor.py   # Data validation and formatting
└── models/
    ├── listing.py      # Listing data model
    ├── enums.py        # Enum definitions
    ├── user_preferences.py
    └── proxy.py
```

## Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd autoscout-scraper
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables**
   ```bash
   cp .env.example .env
   ```
   
   Edit `.env` with your credentials:
   ```env
   SUPABASE_URL=your_supabase_project_url
   SUPABASE_SERVICE_KEY=your_supabase_service_role_key
   TELEGRAM_BOT_TOKEN=your_telegram_bot_token
   TELEGRAM_USER_ID=your_telegram_user_id
   ```

## Usage

### Basic Usage

Run the scraper:
```bash
python main.py
```

The scraper will:
1. Scrape car listings from AutoScout24
2. Process and validate the data
3. Store listings in your Supabase database
4. Link listings to users based on preferences
5. Send a completion report via Telegram

### Configuration

The scraper is configured through the `src/autoscout/models/enums.py` file:

```python
SCRAPING_CONFIG = {
    'max_pages': 20,              # Maximum pages to scrape
    'delay_between_requests': 2,  # Delay between requests (seconds)
    'batch_size': 50              # Batch size for database operations
}
```

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `SUPABASE_URL` | Your Supabase project URL | Yes |
| `SUPABASE_SERVICE_KEY` | Supabase service role key | Yes |
| `TELEGRAM_BOT_TOKEN` | Telegram bot token | Yes |
| `TELEGRAM_USER_ID` | Your Telegram user ID | Yes |

## Database Schema

The scraper expects the following Supabase tables:

### `listings`
- `id` (text, primary key)
- `url` (text)
- `source_site` (text)
- `title` (text)
- `brand` (text)
- `model` (text)
- `year` (date)
- `mileage` (integer)
- `price` (integer)
- `fuel_type` (text)
- `transmission` (text)
- `description` (text)
- `location` (text)
- `source_zipcode_id` (integer, foreign key to zipcodes.id)
- `created_at` (timestamp)

### `users`
- `id` (text, primary key)
- `price_min` (integer)
- `price_max` (integer)
- `mileage_min` (integer)
- `mileage_max` (integer)
- `year_min` (integer)
- `year_max` (integer)

### `user_zipcodes`
- `user_id` (text, foreign key to users.id)
- `zipcode_id` (integer, foreign key to zipcodes.id)

### `zipcodes`
- `id` (integer, primary key)
- `zipcode` (text)
- `city` (text)

### `user_listings`
- `id` (uuid, primary key)
- `user_id` (text, foreign key to users.id)
- `listing_id` (text, foreign key to listings.id)
- `status` (text)
- `created_at` (timestamp)

## Logging

The scraper uses structured logging with the following configuration:

- **Level**: INFO
- **Format**: `%(asctime)s - %(name)s - %(levelname)s - %(message)s`
- **Output**: Console and `autoscout_scraper.log` file

## Error Handling

The scraper includes comprehensive error handling:

- **Network Errors**: Automatic retry with exponential backoff
- **Database Errors**: Fallback from batch to individual insertions
- **Data Validation**: Graceful handling of malformed data
- **Rate Limiting**: Respectful delays between requests

## Monitoring

Monitor the scraper through:

1. **Log Files**: Check `autoscout_scraper.log` for detailed execution logs
2. **Telegram Notifications**: Receive completion reports and error alerts
3. **Database**: Monitor the `listings` table for new entries

## Development

### Code Style

The codebase follows PEP 8 standards with:
- Type hints for all functions
- Comprehensive docstrings
- Consistent naming conventions
- Proper error handling

### Testing

Run tests (if available):
```bash
python -m pytest tests/
```

## Troubleshooting

### Common Issues

1. **Missing Environment Variables**
   - Ensure all required environment variables are set in `.env`
   - Check that Supabase credentials are correct

2. **Database Connection Issues**
   - Verify Supabase URL and service key
   - Ensure database schema is properly set up

3. **Telegram Notifications Not Working**
   - Verify bot token and user ID
   - Ensure bot has permission to send messages

4. **Scraping Rate Limits**
   - Increase `delay_between_requests` in configuration
   - Reduce `max_pages` if needed

### Log Analysis

Check the log file for detailed error information:
```bash
tail -f autoscout_scraper.log
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## Support

For support and questions:
- Check the troubleshooting section
- Review the logs for error details
- Open an issue on GitHub 