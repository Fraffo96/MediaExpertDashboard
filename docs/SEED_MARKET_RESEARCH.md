# Ricerca mercato per il seed (Polonia / EU CE)

Documento per giustificare **ordini di grandezza** usati in [`scripts/seed_catalog/market_reality.py`](../scripts/seed_catalog/market_reality.py). Non sostituisce dati interni Media Expert.

## Limiti

- **Retail totale PL vs specialist CE**: la quota e-commerce sul **retail totale** polacco (~8–9% in fonti aggregate2024) **sottostima** il digitale su elettronica di consumo / elettrodomestici venduti da specialisti e pure player.
- **Western Europe / specialist**: fonti di settore indicano **e-commerce ~circa un terzo** delle vendite di apparecchiature per canali tipo specialist (ordine di grandezza EU; UK più alto).
- **Smartphone PL**: Statcounter misura **base installata / traffico**, non spedizioni; utile come **proxy di popolarità marchio**, non di fatturato.

## Fonti consultate (indicative)

| Tema | Indicazione | Fonte (URL) |
|------|-------------|-------------|
| Mercato CE PL, peso IT/computer | ~34% del valore mercato CE in segmento computer/hardware & software (ordine di grandezza) | [Leading Market Research – Consumer Electronics Retail in Poland](https://www.leadingmarketresearch.com/consumer-electronics-retail-in-poland) |
| Vendor smartphone PL | Apple ~30%, Samsung ~26%, Google ~13%, Xiaomi ~13%, altri (marzo 2026 sulla pagina; serie storica include 2024) | [Statcounter – Mobile vendor Poland](http://gs.statcounter.com/vendor-market-share/mobile/poland/2024) |
| E-commerce vs negozio (EU appliances) | E-commerce **~32%** vendite apparecchiature (WE, contesto Euromonitor citato in sintesi) | Sintesi da ricerca settore (es. [Euromonitor Western Europe appliances](https://www.euromonitor.com/consumer-appliances-in-western-europe/report)) |
| E-commerce share retail totale PL | Dati **non specialist-specific**; usare solo come contro-senso se si alza troppo il digitale | [Statista Poland internet share retail](https://www.statista.com/statistics/1127349/poland-share-of-sales-via-internet-in-retail-sales/) |

## Cosa abbiamo derivato per il codice

1. **Mix macro (parent 1–10)** per **prior** sul catalogo: IT/computer (3–4) rafforzati vs stima CE PL; grandi/small appliances (5–6) non trascurabili (specialist).
2. **Pesi smartphone per brand** in PL (Apple, Samsung, Google, Xiaomi, Motorola, …) allineati a Statcounter dove mappiamo `brand_id`.
3. **Ordini `fact_orders`**: quota “non incollata” al `preferred_channel` aumentata (più rotazione web/app) per avvicinare **~30–35%** ordini digitali complessivi come **proxy specialist**, non retail totale PL.
4. **`gross_pln` ordine**: fascia estesa (bianchi/mobility costosi) oltre i 150–750 PLN fissi.

Aggiornare questo file quando si disponga di dati interni (mix canale, AOV, categoria).
