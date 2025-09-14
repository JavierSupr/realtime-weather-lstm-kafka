import psycopg2
import pandas as pd
import numpy as np
import time
import schedule
import matplotlib.pyplot as plt
from keras.models import Sequential
from keras.layers import LSTM, Dense, Bidirectional
from sklearn.preprocessing import MinMaxScaler

DB_CONFIG = {
    "dbname": "weather-data",
    "user": "postgres",
    "password": "Javier@20",
    "host": "localhost",
    "port": 5432
}

def get_data():
    conn = psycopg2.connect(**DB_CONFIG)
    query = "SELECT ts, weather FROM weather_data ORDER BY ts ASC;"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def prepare_data(df, time_steps=24):
    scaler = MinMaxScaler()
    scaled = scaler.fit_transform(df[['weather']])

    X, y = [], []
    for i in range(len(scaled) - time_steps):
        X.append(scaled[i:i+time_steps])
        y.append(scaled[i+time_steps])
    return np.array(X), np.array(y), scaler

def create_model(input_shape):
    model = Sequential([
        Bidirectional(LSTM(128, input_shape=(input_shape))),
        Dense(100),
        Dense(1)
    ])

    model.compile(optimizer="adam", loss="mse")
    return model


def train_and_predict():
    print("Retrain model...")
    df = get_data()

    time_steps = 24   
    n_forecast = 24   

    X, y, scaler = prepare_data(df, time_steps)

    model = create_model((time_steps, 1))

    model.fit(X, y, epochs=10, batch_size=32, verbose=1)

    history = df['weather'].values[:-n_forecast]
    ts_history = df['ts'].values[:-n_forecast]
    ts_future = df['ts'].values[-n_forecast:] 
    unseen_actual = df['weather'].values[-n_forecast:]

    n_prior = len(history)

    input_seq = X[-1].reshape(time_steps,).tolist()
    preds = []

    for i in range(n_forecast):
        x_input = np.array(input_seq).reshape(1, time_steps, 1)
        pred = model.predict(x_input, verbose=0)[0, 0]
        preds.append(pred)
        input_seq = input_seq[1:] + [pred]

    preds = scaler.inverse_transform(np.array(preds).reshape(-1,1)).flatten()
    y = scaler.inverse_transform(y)

    plt.figure(figsize=(15,6))
    plt.plot(ts_history, history, 'b-', label="History Data")
    plt.plot(ts_future, unseen_actual, 'g-', alpha=0.6, label="Unseen Data")
    plt.plot(ts_future, preds, 'r-', label="Prediction")
    plt.axvline(ts_future[0], color='k', linestyle="--", linewidth=1)
    plt.legend()
    plt.title("Realtime LSTM Forecasting (Weather Data)")
    plt.xlabel("Timestamp")
    plt.ylabel("Weather Value")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()
    
schedule.every(1).seconds.do(train_and_predict)

print("Starting Realtime Prediction...")
while True:
    schedule.run_pending()
    time.sleep(10)