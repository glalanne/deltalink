# DeltaLink

**DeltaLink** is a lightweight and efficient solution for performing basic operations and queries on Delta Lake tables stored in Azure Data Lake Storage (ADLS), leveraging the power of **Unity Catalog** for secure and unified data governance.

## 🚀 Features

- 🔍 Query Delta tables directly from ADLS storage, Managed or External Delta table
- 🛠️ Perform basic CRUD operations (Create, Read, Update, Delete) Delta table
- 🔐 Integrates with Unity Catalog for fine-grained access control
- ⚡ Optimized for performance and scalability using [Daft](https://www.getdaft.io/daft) and [Ray](https://www.ray.io/)


## 📦 Use Cases

- Data exploration and validation in Delta Lake tables
- Lightweight data engineering workflows
- CRUD over REST endpoint

## 🧰 Tech Stack

- **Azure Data Lake Storage (ADLS)**
- **Daft** (with Delta table and UC support)
- **Fast API** (Serving REST API with openspec v3)
- **Poetry** (Python project manager)
- **Ray** (Optional used for the SQL endpoint)
- **Unity Catalog** for access control

## 📁 Project Structure

```
/
├── deltalink/              # Python module
├── tests/                  # Unit and integration tests
├── pyproject.toml
└── README.md
```

## ⚙️ Installation

1. Clone the repository:

```bash
git clone https://github.com/your-username/deltalink.git
cd deltalink
```

2. Install dependencies:

```bash
poetry install
```

3. Configure credentials and endpoints.

## 🧪 Example Usage

```python
# TODO
```

## 🔐 Unity Catalog Integration

```python
# TODO
```

## 🧪 Testing

Run tests using:

```bash
pytest tests/
```

## 📄 License

This project is licensed under the MIT License. See the LICENSE file for details.

## 🙌 Contributing

Contributions are welcome! Please open issues or submit pull requests for improvements or new features.
