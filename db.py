from sqlalchemy import create_engine
from urllib.parse import quote_plus

password = quote_plus("1234")

engine = create_engine(
    f"mysql+pymysql://root:{password}@localhost:3306/de_practice"
)