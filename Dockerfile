FROM python:3.10
ENV PYTHONUNBUFFERED True
ENV APP_HOME /app
ENV VIRTUAL_ENV $APP_HOME/.venv

RUN python -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
COPY ./ERPSincronitzarConsumer/ $APP_HOME/
RUN pip install --upgrade pip

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update && apt-get install -y unixodbc-dev unixodbc odbcinst odbcinst1debian2 && ACCEPT_EULA=Y apt-get install -y msodbcsql18    

RUN pip install -r $APP_HOME/requirements.txt
CMD python $APP_HOME/ERPSincronitzarConsumer.py