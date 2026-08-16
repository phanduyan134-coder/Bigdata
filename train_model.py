import json

from src.air_quality import train_model


if __name__ == "__main__":
    metrics = train_model()
    print(json.dumps(metrics, indent=2))

if __name__ == "__main__":
    metrics = train_model()
    print(json.dumps(metrics, indent=2, ensure_ascii=False))

