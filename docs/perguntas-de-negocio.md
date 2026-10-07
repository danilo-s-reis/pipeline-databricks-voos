# Perguntas de negócio

Todo o pipeline existe para responder estas próximas cinco perguntas. Elas são o critério de
sucesso do projeto — não "o pipeline rodou", mas "o produto de dados responde".

| id | pergunta | o que exige do modelo |
|----|----------|------------------------|
| **P1** | Quais aeroportos concentram os maiores atrasos de partida no Brasil? | atraso de partida em minutos + nome do aeroporto de origem |
| **P2** | Como o atraso evolui ao longo do dia — quanto do atraso noturno é herdado da manhã? | hora prevista de partida + atraso médio por hora |
| **P3** | Qual companhia entrega melhor pontualidade e menor taxa de cancelamento? | nome da companhia + flag de pontualidade + situação do voo + volume de etapas |
| **P4** | Voos internacionais atrasam mais que domésticos? Quanto? | tipo de linha por extenso + atraso |
| **P5** | Quanto atraso as companhias recuperam em voo? | minutos recuperados em voo (atraso de partida − atraso de chegada) |

## Critério de aceitação

O Genie Agent responde corretamente às 5 perguntas, com SQL correto e **nomes legíveis**
(companhias e aeroportos por nome, não por código ICAO). Cada resposta do Genie é
conferida contra uma query SQL escrita à mão.
