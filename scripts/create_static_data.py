
from enterprise_data_trust_poc.data_builder import describe_assets

if __name__ == "__main__":
    assets = describe_assets()
    print("Static sample data is already packaged with the project:")
    for k, v in assets.items():
        print(f"- {k}: {v}")
