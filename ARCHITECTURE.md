## 1. Arquitetura

### 1.1 User Stories

* **US1 (Cálculo de Saldo):** Como gestor da cooperativa, quero que o sistema calcule e registe o saldo energético líquido ($\text{produção total} - \text{consumo total}$) no encerramento de cada período de 15 minutos, ignorando leituras duplicadas e não alterando saldos já fechados com leituras atrasadas.
* **US2 (Notificação e Painel de Excedentes):** Como membro da comunidade, quero ser avisado e ver refletido num painel público sempre que a comunidade entra ou sai de um estado de excedente energético ($\text{saldo} > 0$), sem notificações repetidas enquanto o estado não mudar.
* **US3 (Deteção de Anomalias de Produção):** Como gestor da cooperativa, quero que o sistema detete automaticamente no fecho de cada período se um telhado está a produzir $40\%$ ou mais abaixo da média dos seus vizinhos de rua (desde que existam pelo menos 3 vizinhos com leitura), ignorando quedas generalizadas a toda a rua.
* **US4 (Triagem de Causa por IA):** Como gestor da cooperativa, quero que cada anomalia detetada seja cruzada com o histórico recente do telhado (última hora), os dados dos vizinhos e os relatos em texto livre dos moradores para classificar a causa provável (`sombra`, `sujidade` ou `avaria`) com a respetiva justificação.
* **US5 (Execução Determinística de Ações):** Como gestor da cooperativa, quero que o sistema execute automaticamente a ação correspondente à causa classificada — ignorar se for `sombra`, avisar o membro para limpar o painel se for `sujidade`, ou registar um pedido de intervenção técnica se for `avaria` — garantindo que o texto dos relatos nunca manipula diretamente a decisão operacional.

---

### 1.2 Características Arquiteturais Principais

A arquitetura combina **Microsserviços** comunicando através de uma **Arquitetura Orientada a Eventos (EDA) com topologia Broker** (RabbitMQ), **CQRS** (vistas locais materializadas por serviço) e **Isolamento em Sandbox** para o componente de IA. Esta estrutura é justificada por duas características arquiteturais fundamentais extraídas do domínio:

| Característica Arquitetural | Frase do Enunciado de Onde Sai | Justificação Arquitetural |
| :--- | :--- | :--- |
| **Tolerância a Falhas e Isolamento de Erros** (*Fault Tolerance & Fault Isolation*) | *"integrar uma triagem baseada em IA sem pôr em risco o resto do sistema"* *(reforçada por: "Os valores passam por um gateway por prédio, com rede móvel instável"* e *"F4-Modelo em baixo. F1 e F2 continuam")* | Impõe a separação em **microsserviços distintos** (fronteira física de processo/*bulkhead*), comunicação **assíncrona via Broker** e **vistas locais (CQRS)**. Se o serviço de triagem ou a API externa do LLM falharem, bloquearem ou excederem limites de pedidos, o núcleo operacional não bloqueia nem perde eventos (que ficam retidos nas filas do RabbitMQ). Além disso, o **isolamento em sandbox** confina o modelo não determinístico à emissão de um evento de classificação validado, impedindo-o de executar ações diretas no sistema. |
| **Desacoplamento Temporal, Espacial e de Dados** (*Decoupling*) | *"Avisar os membros quando um excedente começa e acaba, e mostrar essas mudanças num painel."* *(e critério F2: "recebidos por dois consumidores independentes, cada um com a sua fila: aviso e painel")* | Justifica a escolha de uma **topologia Broker (Publish-Subscribe)** e do padrão **CQRS**. O produtor (`Serviço Operacional`) publica factos de domínio uma única vez sem conhecer quem os consome nem depender da disponibilidade imediata dos recetores (desacoplamento espacial e temporal). Paralelamente, o CQRS elimina o acoplamento de esquemas e bases de dados: cada serviço projeta na sua própria base de dados local (SQLite) apenas os dados estruturados para a sua responsabilidade. |

---

### 1.3 Componentes do Sistema

A decomposição dos componentes segue uma abordagem orientada ao **Workflow de Eventos** (e não por *Actor/Action*). Como os fluxos do sistema são reativos e orientados a pipelines temporais (telemetria $\rightarrow$ fecho de período $\rightarrow$ cálculo/deteção $\rightarrow$ triagem $\rightarrow$ reação), agrupar por atores humanos (ex.: `GestorService`, `MoradorService`) misturaria lógica determinística crítica com inferência não determinística de IA no mesmo processo e acoplaria consumidores independentes.

Assim, os componentes são separados pelas etapas do fluxo reativo e respetivos níveis de criticidade, comunicando exclusivamente através de uma *exchange* do tipo `topic` no RabbitMQ com filas duráveis independentes por consumidor:

| Componente Lógico | Contentor (`docker-compose`) | Responsabilidade Principal | Eventos que Subscreve | Eventos que Publica |
| :--- | :--- | :--- | :--- | :--- |
| **Simulador** | `simulador` | Lê ficheiros de cenário e simula a telemetria dos prédios/telhados, relatos de moradores e fecho de períodos, introduzindo atrasos e duplicados. | *(Nenhum — produtor raiz)* | `consumo.medido`<br>`producao.medida`<br>`relato.recebido`<br>`periodo.encerrado` |
| **Gestor de Saldo e Excedente** | `servico-operacional` | Deduplica leituras por `(origem, período)`, calcula o saldo líquido no fecho do período e gere a máquina de estados de excedente. | `consumo.medido`<br>`producao.medida`<br>`periodo.encerrado` | `saldo.calculado`<br>`excedente.iniciado`<br>`excedente.terminado` |
| **Detetor de Anomalias** | `servico-operacional` | Compara a produção de cada telhado com a média simples dos vizinhos da mesma rua no fecho do período ($\ge 3$ vizinhos com leitura e queda $\ge 40\%$). | `producao.medida`<br>`periodo.encerrado` | `producao.anomala` |
| **Executor de Ações** | `servico-operacional` | Módulo *stateless* que mapeia deterministicamente a causa classificada para a ação operacional correspondente (ignorar, avisar membro ou registar pedido de intervenção). | `anomalia.classificada` | *(Emite aviso/registo operacional na consola)* |
| **Triagem IA (Sandbox)** | `servico-triagem` | Componente isolado com vista local CQRS (últimos 4 períodos por telhado/rua e relatos). Classifica anomalias sequencialmente via interface de LLM (com *fallback* determinístico). | `producao.medida`<br>`relato.recebido`<br>`producao.anomala` | `anomalia.classificada` |
| **Consumidor de Avisos** | `consumidor-avisos` | Fila dedicada para emitir notificações destinadas aos moradores da comunidade na consola quando o estado de excedente muda. | `excedente.iniciado`<br>`excedente.terminado` | *(Nenhum — consumidor terminal)* |
| **Consumidor de Painel** | `consumidor-painel` | Fila dedicada para atualizar o estado público do painel informativo da cooperativa na consola de forma independente dos avisos. | `excedente.iniciado`<br>`excedente.terminado`<br>`saldo.calculado` | *(Nenhum — consumidor terminal)* |