FROM continuumio/anaconda3:4.4.0
COPY . /Users/User/PycharmProjects/cementStrengthPrediction/
EXPOSE 8000
WORKDIR /Users/User/PycharmProjects/cementStrengthPrediction/
RUN pip install -r requirements.txt
CMD python main.py