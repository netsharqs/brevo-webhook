import os
import requests
import logging
from datetime import datetime
from dotenv import load_dotenv
import smtplib
from email.message import EmailMessage

load_dotenv() 

API_KEY = os.getenv('BREVO_API_KEY')
SMTP_CONFIG = {
    'server': os.getenv('SMTP_SERVER'),
    'port': os.getenv('SMTP_PORT'),
    'username': os.getenv('SMTP_USERNAME'),
    'password': os.getenv('SMTP_PASSWORD'),
    'sender': os.getenv('EMAIL_SENDER'),
    'recipient': os.getenv('EMAIL_RECIPIENT')
}

API_URL = "https://api.brevo.com/v3"
HEADERS = {
    "api-key": API_KEY,
    "Content-Type": "application/json"
}

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('brevo_automation.log'),
        logging.StreamHandler()
    ]
)

def send_email_notification(subject, body):
    """Sendet eine Email-Benachrichtigung"""
    try:
        msg = EmailMessage()
        msg.set_content(body)
        msg['Subject'] = subject
        msg['From'] = SMTP_CONFIG['sender']
        msg['To'] = SMTP_CONFIG['recipient']

        with smtplib.SMTP(SMTP_CONFIG['server'], SMTP_CONFIG['port']) as server:
            server.starttls()
            server.login(SMTP_CONFIG['username'], SMTP_CONFIG['password'])
            server.send_message(msg)
        logging.info("Email-Benachrichtigung erfolgreich versendet")
    except Exception as e:
        logging.error(f"Fehler beim Senden der Email: {str(e)}")

def get_contacts():
    """Holt alle Kontakte von Brevo"""
    try:
        logging.info("Starte Abruf aller Kontakte")
        response = requests.get(f"{API_URL}/contacts?limit=500", headers=HEADERS)
        response.raise_for_status()
        return response.json().get("contacts", [])
    except requests.exceptions.RequestException as e:
        logging.error(f"Fehler beim Abrufen der Kontakte: {str(e)}")
        return []

def get_company_by_name(company_name):
    """Sucht ein Unternehmen nach Namen"""
    try:
        logging.info(f"Suche nach Unternehmen: {company_name}")
        response = requests.get(f"{API_URL}/companies?name={company_name}", headers=HEADERS)
        response.raise_for_status()
        companies = response.json().get("companies", [])
        return companies[0] if companies else None
    except requests.exceptions.RequestException as e:
        logging.error(f"Fehler bei Unternehmenssuche {company_name}: {str(e)}")
        return None

def create_company(company_name):
    """Erstellt ein neues Unternehmen"""
    try:
        logging.info(f"Erstelle neues Unternehmen: {company_name}")
        payload = {
            "name": company_name,
            "domain": "example.com",
            "type": "customer"
        }
        response = requests.post(f"{API_URL}/companies", json=payload, headers=HEADERS)
        response.raise_for_status()
        logging.info(f"Unternehmen {company_name} erfolgreich erstellt")
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Fehler beim Erstellen des Unternehmens {company_name}: {str(e)}")
        return None

def link_contact_to_company(contact_id, company_id):
    """Verknüpft Kontakt mit Unternehmen"""
    try:
        logging.info(f"Verknüpfe Kontakt {contact_id} mit Unternehmen {company_id}")
        url = f"{API_URL}/companies/{company_id}/contacts"
        payload = {"ids": [contact_id]}
        response = requests.post(url, json=payload, headers=HEADERS)
        
        if response.status_code == 204:
            logging.info("Verknüpfung erfolgreich")
            return True
        else:
            logging.error(f"Verknüpfung fehlgeschlagen: {response.status_code} - {response.text}")
            return False
    except requests.exceptions.RequestException as e:
        logging.error(f"Fehler bei Verknüpfung: {str(e)}")
        return False

def process_contacts():
    """Hauptfunktion zur Verarbeitung aller Kontakte"""
    contacts = get_contacts()
    if not contacts:
        logging.warning("Keine Kontakte gefunden")
        return

    new_contacts = 0
    processed_companies = set()

    for contact in contacts:
        contact_id = contact.get("id")
        email = contact.get("email", "Unbekannt")
        attributes = contact.get("attributes", {})
        company_name = attributes.get("COMPANY")

        # prüft ob Kontakt neu ist 
        created_at = contact.get("createdAt", "")
        is_new = datetime.now().timestamp() - datetime.fromisoformat(created_at).timestamp() < 86400 if created_at else False

        if is_new:
            new_contacts += 1
            logging.info(f"NEUER KONTAKT: {email}")
            send_email_notification(
                "Neuer Kontakt in Brevo",
                f"Neuer Kontakt hinzugefügt:\nEmail: {email}\nUnternehmen: {company_name or 'Kein Unternehmen'}"
            )

        if not company_name:
            logging.info(f"Kein Unternehmen für {email} - überspringe Verknüpfung")
            continue

        # Unternehmen verarbeiten
        if company_name not in processed_companies:
            company = get_company_by_name(company_name)
            if not company:
                company = create_company(company_name)
            
            if company:
                company_id = company.get("id")
                link_contact_to_company(contact_id, company_id)
                processed_companies.add(company_name)

    logging.info(f"Verarbeitung abgeschlossen. {len(contacts)} Kontakte verarbeitet, davon {new_contacts} neu.")

if __name__ == "__main__":
    logging.info("=== Starte Brevo Automatisierung ===")
    process_contacts()
    logging.info("=== Prozess beendet ===")