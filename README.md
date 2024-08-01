Description du Projet

Ce projet est conçu pour automatiser le traitement de fichiers CSV déposés dans un dossier spécifique. Lorsque de nouveaux fichiers sont détectés, Airflow déclenche un pipeline qui produit des messages Kafka à partir des données CSV nettoyées et les envoie à un sujet Kafka. Un consommateur Kafka reçoit ces messages et les insère dans une base de données PostgreSQL.

Fonctionnalités

Surveillance automatique d'un dossier pour les nouveaux fichiers CSV
Nettoyage et transformation des données CSV
Production de messages Kafka
Consommation de messages Kafka
Insertion des données consommées dans PostgreSQL