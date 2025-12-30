import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import json
from datetime import datetime

url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"