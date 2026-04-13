import requests
import json

# --- Configurações ---
page_id = "105415542865629"
# !! IMPORTANTE: Substitua pelo seu Token de Acesso à Página real !!
page_access_token = "EAADhVBrAnNkBOwdOfA8ubmxyrAiKVZBApRUV9LQR8dC5VMnlErsr84szdZB9htkk18PXi0vjLODz07ULFe14gmPykXCwwJGGNSo2QAXZBNasdegnlosofRaASWgzhFNrJA3tKGXc3U7vrXVbkxcmghtdI3diZACxqVuTSpEs677oI374CMBhP4h84iGXSOl57QBR6DYZD"
# Use uma versão recente da API Graph. v22.0 foi usada na doc.
api_version = "v22.0"
# O campo necessário para habilitar a assinatura da página.
# 'feed' é o exemplo comum usado na documentação para habilitar,
# mas os campos específicos do Instagram (como story_insights)
# são configurados no Painel de Apps (Etapa 1 da doc).
subscribed_fields = "feed"

# --- URL da API ---
graph_url = f"https://graph.facebook.com/{api_version}/{page_id}/subscribed_apps"

# --- Payload da Requisição ---
payload = {"subscribed_fields": subscribed_fields, "access_token": page_access_token}

# --- Enviar a Requisição ---
print(f"Tentando habilitar assinaturas para a Página ID: {page_id}...")

try:
    response = requests.post(graph_url, params=payload)
    # Lança uma exceção para respostas de erro (4xx ou 5xx)
    response.raise_for_status()

    response_data = response.json()

    # --- Verificar Resposta ---
    if response_data.get("success"):
        print("\nSucesso! Assinaturas habilitadas para a Página.")
        print("Certifique-se de que:")
        print(
            "1. Seu webhook esteja configurado no Painel de Apps para o objeto 'Instagram'."
        )
        print("2. Você tenha assinado o campo 'story_insights' no Painel de Apps.")
        print(
            "3. Seu App tem as permissões necessárias (instagram_manage_insights, pages_manage_metadata, etc.) com Acesso Avançado."
        )
        print("4. A empresa conectada à Página está verificada.")
    else:
        print("\nFalha ao habilitar assinaturas. Resposta da API:")
        print(json.dumps(response_data, indent=2))

except requests.exceptions.RequestException as e:
    print(f"\nErro na requisição HTTP: {e}")
    if hasattr(e, "response") and e.response is not None:
        try:
            print("Detalhes do erro da API:")
            print(json.dumps(e.response.json(), indent=2))
        except json.JSONDecodeError:
            print("Não foi possível decodificar a resposta de erro como JSON:")
            print(e.response.text)

except Exception as e:
    print(f"\nOcorreu um erro inesperado: {e}")
