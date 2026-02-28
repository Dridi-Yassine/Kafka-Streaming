# Kafka Streaming Lab - Documentation

This document provides installation, configuration, and usage instructions for the Kafka Streaming lab.

## 1. Config

- **Docker Desktop**: Installed and running.
- **Python 3.9+**: Installed on your host machine.
- **Pip**: Python package manager.

## 2. Setup

### A. Environment Setup
1. Create a virtual environment:
   ```powershell
   python -m venv venv
   .\venv\Scripts\activate
   ```
2. Install dependencies:
   ```powershell
   pip install -r requirements.txt
   ```

### B. Kafka Infrastructure
1. Start Kafka and Zookeeper using Docker Compose:
   ```powershell
   docker-compose up -d
   ```
2. Verify that the containers are running:
   ```powershell
   docker ps
   ```

## 3. Lab Part I: CSV Streaming

In this part, we stream raw, unstructured CSV data into Kafka topics.

### Running Part I
1. **Start the Consumer**:
   ```powershell
   python csv_lab/consumer_csv.py
   ```
2. **Start the Producer**:
   ```powershell
   python csv_lab/producer_csv.py
   ```

**Objective**: Observe how Kafka stores and retrieves raw text lines without interpreting their structure.

## 4. Lab Part II: JSON Streaming

In this part, we convert CSV rows into structured JSON objects and implement filtering logic.

### Running Part II
1. **Start the Consumer**:
   ```powershell
   python json_lab/consumer_json.py
   ```
   *Note: This consumer also acts as a processor that routes high-value transactions.*
2. **Start the Producer**:
   ```powershell
   python json_lab/producer_json.py
   ```

### Verifying Filtered Events (Topic Derivation)
To see high-value transactions (amount > 400), run a console consumer:
```powershell
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic transactions-high-value --from-beginning
```

## 5. Takeawyas   

Dans Kafka, les consommateurs s'organisent en "Groupes" (`group_id`). Si un sujet (topic) n'a qu'une seule **partition**, Kafka garantit que cette partition ne sera lue que par **un seul membre** du groupe à la fois.
- **Le problème** : Si un autre processus (background) utilise déjà le même `group_id`, il "verrouille" la lecture. Votre nouveau consommateur restera en attente indéfiniment.
- **Solution** : Fermer les processus fantômes ou utiliser un `group_id` unique pour chaque test indépendant.

Le mode Leader/Follower assure que les données ne sont pas perdues, tandis que le Consumer Group assure que le traitement est réparti efficacement.

1.  **Leader/Follower (Réplication/Broker)** :
    - C'est au niveau de l'infrastructure (les serveurs Kafka).
    - Chaque partition a un **Leader** (le broker qui gère les lectures/écritures) et des **Followers** (qui copient les données pour la sécurité).
    - C'est pour la **Haute Disponibilité**.

2.  **Consumer Groups (Consommation/Client)** :
    - C'est au niveau de l'application.
    - Il n'y a pas de "Follower" ici, mais une **répartition de charge**.
    - Si vous avez 3 partitions et 3 consommateurs dans le même groupe, chaque consommateur devient le "spécialiste" d'une partition.
    - C'est pour le **Passage à l'Échelle (Scalability)**.

## 6. Lab Phase 2: Dirty Data

Cette phase simule des conditions réelles où les données peuvent être corrompues ou incomplètes.

### Objectifs
- Valider les données en temps réel.
- Isoler les erreurs dans une **Dead Letter Queue (DLQ)**.
- Monitorer la qualité via des rapports **KPI** (taux d'erreur).

### Composants
1. **Producer Dirty** : Envoie des données imparfaites (`data/transactions_dirty.csv`).
2. **Validation Consumer** : Valide chaque record, affiche les KPIs, et logue les métriques dans `data_quality_lab/metrics.log`.
3. **DLQ Consumer** : Permet d'inspecter les records rejetés.

### Commandes
1. **Démarrer le Moniteur DLQ** :
   ```powershell
   python data_quality_lab/consumer_dlq.py
   ```
2. **Démarrer le Validateur (Processor)** :
   ```powershell
   python data_quality_lab/consumer_csv.py
   ```
3. **Lancer le Flux Dirty** :
   ```powershell
   python data_quality_lab/producer.py
   ```

Le consumer détecte les `bad_type`, `missing_field` et `bad_format` et les envoie automatiquement au topic `transactions-dlq`.

---
## 8. Réflexions

### Part I — CSV Streaming
- **Pourquoi Kafka ne se soucie-t-il pas de la structure CSV ?**
  Kafka est un "byte-store". Il traite les données comme des tableaux d'octets opaques. Cela permet une performance maximale et une flexibilité totale : c'est au producteur et au consommateur de s'accorder sur le format (Contrat).
- **Quels problèmes pose le CSV dans le streaming ?**
  Le CSV n'est pas typé (tout est string) et n'est pas auto-descriptif. Si une colonne est ajoutée au milieu, tout le pipeline risque de se casser (problème d'indice).
- **Pourquoi les offsets sont-ils critiques ?**
  Ils permettent de garantir qu'aucun message n'est perdu ou traité deux fois en cas de crash du consommateur. Ils marquent la progression dans le flux.

### Part II — JSON Streaming
- **Pourquoi le JSON est-il meilleur que le CSV ?**
  Il est auto-descriptif (clés/valeurs) et supporte des structures imbriquées. Il permet d'évoluer le schéma (ajouter des champs) sans casser les anciens consommateurs.
- **Décision de Schéma (Transactions)** :
  Nous avons choisi :
  - `transaction_id` (string) : Identifiant unique.
  - `user_id` (string) : Pour le partitionnement futur par utilisateur.
  - `amount` (float) : Pour permettre les calculs arithmétiques.
  - `timestamp` (ISO 8601) : Standard pour le tri temporel.
- **Kafka comme "Source of Truth"** :
  Grâce à sa persistance, Kafka permet de "rejouer" (replay) l'histoire plusieurs fois pour reconstruire un état, ce qui est crucial en cas de bug.


---
