"""
Kedro settings for the property-scraper project.
"""

# Project settings
PROJECT_NAME = "property-scraper"
PROJECT_VERSION = "0.1.0"

# Pipeline settings
PIPELINE_AUTO_DISCOVERY_FLAG = True

# Data catalog settings - use new KedroDataCatalog
CONF_SOURCE = "conf"
ENV = "local"

# Disable telemetry
KEDRO_DISABLE_TELEMETRY = True

# Logging settings - use simple format and suppress deprecation warnings
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "simple": {
            "format": "%(message)s"
        },
    },
    "handlers": {
        "console": {
            "level": "INFO",
            "formatter": "simple",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
        },
    },
    "loggers": {
        "kedro": {
            "level": "WARNING",
            "handlers": ["console"],
            "propagate": False
        },
        "kedro.framework": {
            "level": "WARNING",
            "handlers": ["console"],
            "propagate": False
        },
        "kedro.io": {
            "level": "WARNING",
            "handlers": ["console"],
            "propagate": False
        },
        "kedro.runner": {
            "level": "WARNING",
            "handlers": ["console"],
            "propagate": False
        },
        "property_scraper": {
            "level": "INFO",
            "handlers": ["console"],
            "propagate": False
        },
        "": {
            "level": "INFO",
            "handlers": ["console"],
            "propagate": False
        },
    }
}

# Context settings
# CONTEXT_CLASS = KedroContext

# Runner settings
# RUNNER = SequentialRunner()

# Hook settings
HOOKS = () 