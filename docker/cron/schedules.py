"""
Cron schedule configurations for different environments.
This module provides centralized management of cron schedules.

Format explanation:
* * * * *
│ │ │ │ │
│ │ │ │ └── Day of week (0-7, where both 0 and 7 represent Sunday)
│ │ │ └──── Month (1-12)
│ │ └────── Day of month (1-31)
│ └──────── Hour (0-23)
└────────── Minute (0-59)
"""

# Testing schedule - runs every minute
TESTING_SCHEDULE = "* * * * *"

# Production schedule - runs every 6 hours
# This will run at 00:00, 06:00, 12:00, and 18:00
PRODUCTION_SCHEDULE = "0 */6 * * *"

# Development schedule - runs every 5 minutes
DEVELOPMENT_SCHEDULE = "*/5 * * * *"

def get_schedule(environment: str = "testing") -> str:
    """
    Get the appropriate cron schedule based on environment.

    Args:
        environment: The environment name ('testing', 'development', or 'production')

    Returns:
        str: The corresponding cron schedule
    """
    schedules = {
        "testing": TESTING_SCHEDULE,
        "development": DEVELOPMENT_SCHEDULE,
        "production": PRODUCTION_SCHEDULE
    }
    return schedules.get(environment, TESTING_SCHEDULE)