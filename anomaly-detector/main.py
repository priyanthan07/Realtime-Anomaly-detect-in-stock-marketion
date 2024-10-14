import os
from quixstreams import Application
from sklearn.ensemble import IsolationForest
from collections import defaultdict
import numpy as np

# for local dev, load env vars from a .env file
from dotenv import load_dotenv

load_dotenv()

app = Application(consumer_group="transformation-v1", auto_offset_reset="earliest")

input_topic = app.topic(os.environ["input"])
output_topic = app.topic(os.environ["output"])

high_volumn_threshold = defaultdict(lambda: 20000)
fit_prices = []
is_fitted = False

isolation_forest = IsolationForest(contamination=0.01, n_estimators=1000)


def high_volumn_rule(trade_data):
    trade_data["high_volumn_anomaly"] = bool(
        trade_data["size"] > high_volumn_threshold[trade_data["symbol"]]
    )
    return trade_data


def isolation_forest_rule(trade_data):
    global is_fitted
    current_price = trade_data["price"]

    fit_prices.append(float(current_price))

    if len(fit_prices) < 1000:
        trade_data["isolation_fores_anomaly"] = False
        return trade_data

    fit_prices_normalized = (np.array(fit_prices) - np.mean(fit_prices)) / np.std(
        fit_prices
    )
    prices_reshaped = fit_prices_normalized.reshape(-1, 1)

    if len(fit_prices) % 1000 == 0:
        isolation_forest.fit(prices_reshaped)
        is_fitted = True

    if not is_fitted:
        trade_data["isolation_fores_anomaly"] = False
        return trade_data

    current_price_normalized = (current_price - float(np.mean(fit_prices))) / float(
        np.std(fit_prices)
    )
    score = isolation_forest.decision_function([[current_price_normalized]])
    trade_data["isolation_fores_anomaly"] = bool(score < 0)
    return trade_data


def combine_anomalies(trade_data):
    anomalies = []

    if trade_data.get("high_volumn_anomaly"):
        anomalies.append("High Volumn")

    if trade_data.get("isolation_fores_anomaly"):
        anomalies.append("Isolation Forest")

    trade_data["anomalies"] = anomalies if anomalies else None

    return trade_data


if __name__ == "__main__":
    sdf = app.dataframe(input_topic)
    sdf = (
        sdf.apply(high_volumn_rule)
        .apply(isolation_forest_rule)
        .apply(combine_anomalies)
    )
    sdf = sdf.filter(lambda row: row.get("anomalies") and len(row["anomalies"]) >= 1)

    sdf.to_topic(output_topic)
    app.run(sdf)
