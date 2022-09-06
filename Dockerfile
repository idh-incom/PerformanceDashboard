FROM python:3.7.7-buster

ARG GITHUB_TOKEN=e2cef268ef4d7c16858701ee4b0ae1ed9adcbd66
ENV GITHUB_TOKEN=${GITHUB_TOKEN}

# Used in algo_framework to determine what config file is being read
ENV ALGO_FRAMEWORK_CONFIG=production
ENV APP_NAME=gas_flex_tool
ENV APP_AUTHOR=IDH

WORKDIR /app

ADD requirements.txt /app

# Microsoft ODBC
# https://docs.microsoft.com/en-gb/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-2017
RUN su -c "curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -" \
  && su -c "curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list" \
  && apt-get update \
  && ACCEPT_EULA=Y apt-get install msodbcsql17 -y \
  && apt-get install unixodbc-dev -y

RUN python3 -m pip install -r requirements.txt

# Add entire folder into app folder
ADD Dashboard /app/Dashboard
ADD .streamlit /app/.streamlit

# Ensure that you can do e.g. "from flex_tool.pages import required_spread_breakeven"
ENV PYTHONPATH "${PYTHONPATH}:/app"

EXPOSE 8501

CMD streamlit run "Dashboard/website_local.py"
