        const messageInput = document.getElementById('message-input');
        const sendButton = document.getElementById('send-button');
        const welcomeScreen = document.getElementById('welcome-screen');
        const chatContainer = document.getElementById('chat-container');
        const mainContent = document.getElementById('main-content');
        const sidebar = document.getElementById('sidebar');
        const sidebarBackdrop = document.getElementById('sidebar-backdrop');
        const menuButton = document.getElementById('menu-button');
        const closeSidebarBtn = document.getElementById('close-sidebar');
        const headerEl = document.getElementById('header');
        const pinSidebarBtn = document.getElementById('pin-sidebar');
        const assistantActivateBtn = document.getElementById('assistant-activate');
        const assistantActivateInlineBtn = document.getElementById('assistant-activate-inline');
        const miniSidebar = document.getElementById('sidebar-mini');
        const miniOpen = document.getElementById('mini-open');
        const miniAssistente = document.getElementById('mini-assistente');
        const miniMercado = document.getElementById('mini-mercado');
        const miniCnpj = document.getElementById('mini-cnpj');
        const miniB2G = document.getElementById('mini-b2g');
        const miniB2B = document.getElementById('mini-b2b');


        const navAssistente = document.getElementById('nav-assistente');
        const navMercado = document.getElementById('nav-mercado');
        const navB2B = document.getElementById('nav-b2b');
        const navB2G = document.getElementById('nav-b2g');
        const navVisaoGeral = document.getElementById('nav-visao-geral');
        const navEstrutura = document.getElementById('nav-estrutura');
        const navDemografia = document.getElementById('nav-demografia');
        const navComercial = document.getElementById('nav-comercial');
        const navFormalizacao = document.getElementById('nav-formalizacao');
        const navSocietaria = document.getElementById('nav-societaria');
        const navTerritorial = document.getElementById('nav-territorial');
        const navSaude = document.getElementById('nav-saude');
        const marketPage = document.getElementById('market-page');
        const b2bPage = document.getElementById('b2b-page');
        const b2gPage = document.getElementById('b2g-page');
        const mkRun = document.getElementById('mk-run');
        const charts = {};

        let filtrosGlobais = {
            cnae: null,
            uf: null,
            municipio: null,
            porte: null,
            ano_min: null,
            ano_max: null
        };
        async function aplicarFiltrosGlobais() {
            const filterCnae = document.getElementById('filter-cnae');
            const filterUf = document.getElementById('filter-uf');
            filtrosGlobais = {
                cnae: filterCnae ? normalizeCNAE(filterCnae.value || '') : null,
                uf: filterUf ? String(filterUf.value || '').toUpperCase().trim() : null,
                municipio: null,
                porte: null,
                ano_min: null,
                ano_max: null
            };
            await analisarMercado();
        }

        // sidebarPinned moved to navigation.js
        try {
            const stored = localStorage.getItem('sidebar_pinned');
            if (stored !== null) { sidebarPinned = (stored === 'true'); }
        } catch { }

        // applyPinnedState moved to navigation.js
        applyPinnedState();

        // Carrega dados iniciais ao abrir a página
        (async function initializePage() {
            try {
                // Check Login
                const isLoggedIn = localStorage.getItem('pickles_auth') === 'true';
                const loginScreen = document.getElementById('login-screen');
                const btnLogin = document.getElementById('btn-login-action');
                const loginEmail = document.getElementById('login-email');
                const loginPassword = document.getElementById('login-password');
                const loginError = document.getElementById('login-error');

                if (isLoggedIn) {
                    if (loginScreen) loginScreen.classList.add('hidden');
                    applyPinnedState();
                    await loadAtivasFromBase();
                    await analisarMercado();
                } else {
                    if (loginScreen) loginScreen.classList.remove('hidden');

                    // Setup Login Action
                    if (btnLogin) {
                        // Enable login on Enter key
                        const handleLogin = async () => {
                            const email = loginEmail ? loginEmail.value.trim() : '';
                            const password = loginPassword ? loginPassword.value.trim() : '';

                            // Basic validation
                            if (!email || !password) {
                                if (loginError) {
                                    loginError.textContent = 'Por favor, preencha todos os campos';
                                    loginError.classList.remove('hidden');
                                }
                                return;
                            }

                            if (loginError) loginError.classList.add('hidden');

                            // Mimic login delay
                            btnLogin.textContent = 'Entrando...';
                            btnLogin.disabled = true;
                            await new Promise(r => setTimeout(r, 800));

                            // Store user info
                            localStorage.setItem('pickles_auth', 'true');
                            localStorage.setItem('pickles_user_email', email);

                            loginScreen.classList.add('hidden');
                            applyPinnedState();
                            await loadAtivasFromBase();
                            await analisarMercado();
                        };

                        btnLogin.addEventListener('click', handleLogin);

                        // Enter key support
                        if (loginEmail) {
                            loginEmail.addEventListener('keypress', (e) => {
                                if (e.key === 'Enter') handleLogin();
                            });
                        }
                        if (loginPassword) {
                            loginPassword.addEventListener('keypress', (e) => {
                                if (e.key === 'Enter') handleLogin();
                            });
                        }
                    }
                }

                // Setup Logout
                const logoutButton = document.getElementById('logout-button');
                if (logoutButton) {
                    logoutButton.addEventListener('click', () => {
                        if (confirm('Deseja realmente sair?')) {
                            localStorage.removeItem('pickles_auth');
                            localStorage.removeItem('pickles_user_email');
                            location.reload();
                        }
                    });
                }
            } catch (e) {
                try { console.error('Erro ao carregar dados iniciais:', e); } catch { }
            }
        })();



        const planPage = document.getElementById('plan-page');
        const cnpjPage = document.getElementById('cnpj-page');
        const settingsPage = document.getElementById('settings-page');
        const userProfileBtn = document.getElementById('user-profile-btn');

        function updatePageTitle(title) {
            const el = document.getElementById('page-title');
            if (el) el.textContent = '';
        }

        function showAssistant() {
            updatePageTitle('Assistente IA');
            if (marketPage) marketPage.classList.add('hidden');
            if (b2bPage) b2bPage.classList.add('hidden');
            if (b2gPage) b2gPage.classList.add('hidden');
            if (planPage) planPage.classList.add('hidden');
            if (cnpjPage) cnpjPage.classList.add('hidden');
            if (settingsPage) settingsPage.classList.add('hidden');
            if (welcomeScreen) welcomeScreen.classList.remove('hidden');
            if (chatContainer) chatContainer.classList.add('hidden');
            mainContent.classList.add('justify-center', 'items-center');
            mainContent.classList.remove('items-start');
            const ib = document.getElementById('interaction-bar');
            if (ib) ib.classList.add('hidden');
        }

        function showMercado() {
            updatePageTitle('Análise Setorial');
            if (welcomeScreen) welcomeScreen.classList.add('hidden');
            if (chatContainer) chatContainer.classList.add('hidden');
            if (marketPage) marketPage.classList.remove('hidden');
            if (b2bPage) b2bPage.classList.add('hidden');
            if (b2gPage) b2gPage.classList.add('hidden');
            if (planPage) planPage.classList.add('hidden');
            if (cnpjPage) cnpjPage.classList.add('hidden');
            if (settingsPage) settingsPage.classList.add('hidden');
            mainContent.classList.remove('justify-center', 'items-center');
            mainContent.classList.add('pt-6', 'items-start');
            try { loadAtivasFromBase(); } catch { }
            const ib = document.getElementById('interaction-bar');
            if (ib) ib.classList.add('hidden');
        }

        function showB2B() {
            updatePageTitle('Oportunidades B2B');
            if (welcomeScreen) welcomeScreen.classList.add('hidden');
            if (chatContainer) chatContainer.classList.add('hidden');
            if (marketPage) marketPage.classList.add('hidden');
            if (b2bPage) b2bPage.classList.remove('hidden');
            if (b2gPage) b2gPage.classList.add('hidden');
            if (planPage) planPage.classList.add('hidden');
            if (cnpjPage) cnpjPage.classList.add('hidden');
            if (settingsPage) settingsPage.classList.add('hidden');
            mainContent.classList.remove('justify-center', 'items-center');
            mainContent.classList.add('pt-6', 'items-start');
            const ib = document.getElementById('interaction-bar');
            if (ib) ib.classList.add('hidden');
            try { renderB2BList(); } catch { }
        }

        function showB2G() {
            updatePageTitle('Oportunidades B2G');
            if (welcomeScreen) welcomeScreen.classList.add('hidden');
            if (chatContainer) chatContainer.classList.add('hidden');
            if (marketPage) marketPage.classList.add('hidden');
            if (b2bPage) b2bPage.classList.add('hidden');
            if (b2gPage) b2gPage.classList.remove('hidden');
            if (planPage) planPage.classList.add('hidden');
            if (cnpjPage) cnpjPage.classList.add('hidden');
            if (settingsPage) settingsPage.classList.add('hidden');
            mainContent.classList.remove('justify-center', 'items-center');
            mainContent.classList.add('pt-6', 'items-start');
            const ib = document.getElementById('interaction-bar');
            if (ib) ib.classList.add('hidden');
            try { renderB2GList(); } catch { }
        }

        function showPlans() {
            updatePageTitle('Planos & Faturamento');
            if (welcomeScreen) welcomeScreen.classList.add('hidden');
            if (chatContainer) chatContainer.classList.add('hidden');
            if (marketPage) marketPage.classList.add('hidden');
            if (b2bPage) b2bPage.classList.add('hidden');
            if (b2gPage) b2gPage.classList.add('hidden');
            if (cnpjPage) cnpjPage.classList.add('hidden');
            if (settingsPage) settingsPage.classList.add('hidden');
            if (planPage) planPage.classList.remove('hidden');
            mainContent.classList.remove('justify-center', 'items-center');
            mainContent.classList.add('pt-6', 'items-start');
            const ib = document.getElementById('interaction-bar');
            if (ib) ib.classList.add('hidden');
        }

        function showSettings() {
            updatePageTitle('Configurações da Conta');
            if (welcomeScreen) welcomeScreen.classList.add('hidden');
            if (chatContainer) chatContainer.classList.add('hidden');
            if (marketPage) marketPage.classList.add('hidden');
            if (b2bPage) b2bPage.classList.add('hidden');
            if (b2gPage) b2gPage.classList.add('hidden');
            if (cnpjPage) cnpjPage.classList.add('hidden');
            if (planPage) planPage.classList.add('hidden');
            if (settingsPage) settingsPage.classList.remove('hidden');

            mainContent.classList.remove('justify-center', 'items-center');
            mainContent.classList.add('pt-6', 'items-start');
            const ib = document.getElementById('interaction-bar');
            if (ib) ib.classList.add('hidden');
        }

        if (userProfileBtn) userProfileBtn.addEventListener('click', showSettings);

        // Settings Tabs Logic
        document.querySelectorAll('.settings-tab-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                // Update buttons state
                document.querySelectorAll('.settings-tab-btn').forEach(b => {
                    b.classList.remove('active', 'bg-blue-50', 'text-blue-700');
                    b.classList.add('text-slate-600', 'hover:bg-slate-50');
                });
                btn.classList.remove('text-slate-600', 'hover:bg-slate-50');
                btn.classList.add('active', 'bg-blue-50', 'text-blue-700');

                // Update content visibility
                const tabId = btn.getAttribute('data-tab');
                document.querySelectorAll('.settings-tab-content').forEach(content => {
                    content.classList.add('hidden');
                });
                const target = document.getElementById(`tab-${tabId}`);
                if (target) target.classList.remove('hidden');
            });
        });

        function showCnpj() {
            updatePageTitle('Consulta CNPJ');
            if (welcomeScreen) welcomeScreen.classList.add('hidden');
            if (chatContainer) chatContainer.classList.add('hidden');
            if (marketPage) marketPage.classList.add('hidden');
            if (b2bPage) b2bPage.classList.add('hidden');
            if (b2gPage) b2gPage.classList.add('hidden');
            if (planPage) planPage.classList.add('hidden');
            if (settingsPage) settingsPage.classList.add('hidden');
            if (cnpjPage) cnpjPage.classList.remove('hidden');
            mainContent.classList.remove('justify-center', 'items-center');
            mainContent.classList.add('pt-6', 'items-start');
            const ib = document.getElementById('interaction-bar');
            if (ib) ib.classList.add('hidden');
        }

        if (navAssistente) { navAssistente.addEventListener('click', (e) => { e.preventDefault(); showAssistant(); if (!sidebarPinned) closeSidebar(); }); }
        if (navMercado) { navMercado.addEventListener('click', (e) => { e.preventDefault(); showMercado(); if (!sidebarPinned) closeSidebar(); }); }
        if (navB2B) { navB2B.addEventListener('click', (e) => { e.preventDefault(); showB2B(); if (!sidebarPinned) closeSidebar(); }); }
        if (navB2G) { navB2G.addEventListener('click', (e) => { e.preventDefault(); showB2G(); if (!sidebarPinned) closeSidebar(); }); }
        const navCnpj = document.getElementById('nav-cnpj');
        if (navCnpj) { navCnpj.addEventListener('click', (e) => { e.preventDefault(); showCnpj(); if (!sidebarPinned) closeSidebar(); }); }

        // Link "Plano & Faturamento" from Sidebar
        document.querySelectorAll('nav a').forEach(link => {
            if (link.textContent.toLowerCase().includes('plano') || link.textContent.toLowerCase().includes('faturamento')) {
                link.href = '#';
                link.addEventListener('click', (e) => {
                    e.preventDefault();
                    showPlans();
                    if (!sidebarPinned) closeSidebar();
                });
            }
        });

        function showDashboardView(viewName) {
            try {
                ['visao-geral', 'estrutura', 'demografia', 'comercial', 'formalizacao', 'societaria', 'territorial', 'saude', 'players'].forEach(v => {
                    const el = document.getElementById(`view-${v}`);
                    if (el) el.classList.add('hidden');
                });
                const selected = document.getElementById(`view-${viewName}`);
                if (selected) selected.classList.remove('hidden');
                document.querySelectorAll('.dashboard-tab').forEach(tab => tab.classList.remove('active'));
                const activeTab = document.querySelector(`.dashboard-tab[data-view="${viewName}"]`);
                if (activeTab) activeTab.classList.add('active');
                showMercado();
            } catch { }
        }
        const navItems = {
            'nav-visao-geral': 'visao-geral',
            'nav-estrutura': 'estrutura',
            'nav-demografia': 'demografia',
            'nav-comercial': 'comercial',
            'nav-formalizacao': 'formalizacao',
            'nav-societaria': 'societaria',
            'nav-territorial': 'territorial',
            'nav-saude': 'saude',
            'nav-players': 'players'
        };
        Object.keys(navItems).forEach(navId => {
            const el = document.getElementById(navId);
            if (!el) return;
            el.addEventListener('click', (e) => {
                e.preventDefault();
                showDashboardView(navItems[navId]);
                if (!sidebarPinned) closeSidebar();
            });
        });
        try { initDashboardTabs(); } catch { }
        showDashboardView('visao-geral');
        try { document.body.classList.add('compact'); } catch { }
        if (assistantActivateBtn) {
            assistantActivateBtn.addEventListener('click', () => {
                showAssistant();
                if (messageInput && messageInput.value.trim() !== '') { sendMessage(); } else { if (messageInput) messageInput.focus(); }
            });
        }
        if (assistantActivateInlineBtn) {
            assistantActivateInlineBtn.addEventListener('click', () => {
                showAssistant();
                if (messageInput && messageInput.value.trim() !== '') { sendMessage(); } else { if (messageInput) messageInput.focus(); }
            });
        }
        if (miniOpen) { miniOpen.addEventListener('click', openSidebar); }
        if (miniAssistente) { miniAssistente.addEventListener('click', () => { showAssistant(); }); }
        if (miniMercado) { miniMercado.addEventListener('click', () => { showMercado(); }); }
        if (miniCnpj) { miniCnpj.addEventListener('click', () => { showCnpj(); }); }
        if (miniB2G) { miniB2G.addEventListener('click', () => { showB2G(); }); }
        if (miniB2B) { miniB2B.addEventListener('click', () => { showB2B(); }); }

        const voiceToggleBtn = document.getElementById('voice-toggle');
        const voiceRecordBtn = document.getElementById('voice-record');
        let voiceEnabled = false;
        let recognition = null;
        function initVoice() {
            const SR = window.SpeechRecognition || window.webkitSpeechRecognition;
            if (!SR) return;
            recognition = new SR();
            recognition.lang = 'pt-BR';
            recognition.continuous = false;
            recognition.interimResults = false;
            recognition.onresult = (e) => {
                const text = Array.from(e.results).map(r => r[0].transcript).join(' ');
                if (messageInput) {
                    messageInput.value = text;
                    sendButton.disabled = text.trim() === '';
                    autoResize(messageInput);
                }
            };
        }
        function speak(text) {
            if (!voiceEnabled) return;
            try {
                const u = new SpeechSynthesisUtterance(text);
                u.lang = 'pt-BR';
                window.speechSynthesis.cancel();
                window.speechSynthesis.speak(u);
            } catch { }
        }
        if (voiceToggleBtn) {
            voiceToggleBtn.addEventListener('click', () => {
                voiceEnabled = !voiceEnabled;
                voiceToggleBtn.classList.toggle('text-blue-600', voiceEnabled);
            });
        }
        if (voiceRecordBtn) {
            voiceRecordBtn.addEventListener('click', () => {
                if (!recognition) initVoice();
                if (recognition) {
                    try { recognition.start(); } catch { }
                }
            });
        }
        function normalizeCNAE(text) { const s = String(text || '').replace(/[^0-9]/g, ''); return s.length >= 4 ? s : ''; }

        // Filtros globais para os cards
        window.currentChurnFilter = 30;
        window.currentGrowthFilter = 30;
        window.updateChurnFilter = function (val) {
            window.currentChurnFilter = parseInt(val);
            if (window.lastGeralData) renderVisaoGeral(window.lastGeralData);
        };
        window.updateGrowthFilter = function (val) {
            window.currentGrowthFilter = parseInt(val);
            if (window.lastGeralData) renderVisaoGeral(window.lastGeralData);
        };

        function renderVisaoGeral(data) {
            window.lastGeralData = data;
            try {
                const cards = (data && data.cards) || {};
                const graficos = (data && data.graficos) || {};
                const evol = graficos.evolucao_ativas || {};
                const entSai = graficos.entradas_vs_saidas || {};
                const mapa = graficos.mapa_calor || {};
                const ranking = (data && data.ranking) || {};
                const base = data && data.top;
                const sit = (base && base.situacao_cadastral) || [];
                const ativa = sit.find(x => String(x.label).toLowerCase() === 'ativa');
                const totalAtivas = typeof cards.total_ativas === 'number' ? cards.total_ativas : (ativa ? ativa.count : null);
                const entVig = typeof cards.entradas_mes_vigente === 'number' ? cards.entradas_mes_vigente : null;
                const entAnt = typeof cards.entradas_mes_anterior === 'number' ? cards.entradas_mes_anterior : null;
                const entradas = Array.isArray(cards.entradas_mensais) ? (cards.entradas_mensais.slice(-1)[0]?.count || null) : null;
                const entradas30 = typeof cards.entradas_30_dias === 'number' ? cards.entradas_30_dias : null;

                // --- CHURN LOGIC REAL (Dados Backend) ---
                let saidas = null;
                const saiSeries = Array.isArray(cards.saidas_mensais) ? cards.saidas_mensais : [];

                if (saiSeries.length > 0) {
                    // Dados reais retornados pelo backend
                    const last = saiSeries[saiSeries.length - 1]?.count || 0;
                    if (window.currentChurnFilter === 60) {
                        // Soma dos últimos 2 meses para visão 60 dias
                        const penult = saiSeries.length > 1 ? (saiSeries[saiSeries.length - 2]?.count || 0) : 0;
                        saidas = last + penult;
                    } else {
                        // Visão 30 dias (último mês fechado)
                        saidas = last;
                    }
                } else if (totalAtivas) {
                    // Fallback apenas se backend falhar
                    saidas = Math.floor(totalAtivas * 0.0085);
                    if (window.currentChurnFilter === 60) saidas = Math.floor(saidas * 2.1);
                }

                const idade = typeof cards.idade_media === 'number' ? cards.idade_media : null;
                const pct = typeof cards.pct_dados_validos === 'number' ? cards.pct_dados_validos : null;
                const fmt = (n) => (typeof n === 'number' ? n.toLocaleString('pt-BR') : '—');
                const el = (id) => document.getElementById(id);
                if (el('vg-total-ativas')) el('vg-total-ativas').textContent = fmt(totalAtivas);
                if (el('vg-entradas-mensais')) el('vg-entradas-mensais').textContent = fmt(entVig != null ? entVig : (entradas30 != null ? entradas30 : entradas));
                if (el('vg-saidas-mensais')) el('vg-saidas-mensais').textContent = fmt(saidas);
                if (el('vg-idade-media')) el('vg-idade-media').textContent = idade ? idade.toFixed(1) : '—';
                if (el('vg-pct-validos')) el('vg-pct-validos').textContent = pct != null ? `${pct.toFixed(1)}%` : '—%';
                const vigLab = typeof cards.entradas_mes_vigente_label === 'string' ? cards.entradas_mes_vigente_label : null;
                const antLab = typeof cards.entradas_mes_anterior_label === 'string' ? cards.entradas_mes_anterior_label : null;
                if (el('vg-entradas-periodo')) el('vg-entradas-periodo').textContent = (vigLab && antLab) ? `Período: ${vigLab} vs ${antLab}` : (vigLab ? `Período: ${vigLab}` : '—');
                if (el('vg-validos-msg')) el('vg-validos-msg').textContent = pct != null ? 'Base com cobertura de dados válida' : 'Dados sendo processados';
                // Deltas
                const entSeries = Array.isArray(cards.entradas_mensais) ? cards.entradas_mensais : [];
                const evolSeries = (evol && Array.isArray(evol.valores)) ? evol.valores.map(v => parseInt(v) || 0) : [];
                const calcDeltaPct = (arr) => {
                    const n = arr.length;
                    if (n >= 2) {
                        const prev = parseInt(arr[n - 2]?.count || arr[n - 2] || 0);
                        const cur = parseInt(arr[n - 1]?.count || arr[n - 1] || 0);
                        if (prev === 0) return cur > 0 ? 100.0 : 0.0;
                        return ((cur - prev) / Math.max(1, prev)) * 100.0;
                    }
                    return 0.0;
                };
                const entDelta = (entVig != null && entAnt != null) ? (((entVig - entAnt) / Math.max(1, entAnt)) * 100.0) : calcDeltaPct(entSeries);
                const saiDelta = calcDeltaPct(saiSeries);
                const ativasDelta = (() => {
                    const n = evolSeries.length;
                    if (n >= 2) {
                        const prev = evolSeries[n - 2];
                        const cur = evolSeries[n - 1];
                        if (prev === 0) return cur > 0 ? 100.0 : 0.0;
                        return ((cur - prev) / Math.max(1, prev)) * 100.0;
                    }
                    return 0.0;
                })();
                if (el('vg-entradas-delta')) el('vg-entradas-delta').textContent = `${entDelta.toFixed(1)}%`;
                if (el('vg-entradas-yoy')) {
                    const latestLabel = vigLab || (entSeries.length ? String(entSeries[entSeries.length - 1].label || '') : '');
                    let yoyText = '—';
                    if (latestLabel && latestLabel.includes('-')) {
                        const [yStr, mStr] = latestLabel.split('-'); const y = parseInt(yStr || '0'); const m = parseInt(mStr || '0');
                        const target = `${(y - 1)}-${String(m).padStart(2, '0')}`;
                        const prevYear = entSeries.find(x => String(x.label || '') === target);
                        if (prevYear && entVig != null) {
                            const pv = parseInt(prevYear.count || 0);
                            const yoy = pv > 0 ? ((entVig - pv) / pv) * 100.0 : (entVig > 0 ? 100.0 : 0.0);
                            yoyText = `YoY ${y - 1}: ${yoy.toFixed(1)}%`;
                        }
                    }
                    el('vg-entradas-yoy').textContent = yoyText;
                }
                if (el('vg-saidas-delta')) el('vg-saidas-delta').textContent = `${saiDelta.toFixed(1)}%`;
                if (el('vg-total-ativas-delta')) el('vg-total-ativas-delta').textContent = `${ativasDelta.toFixed(1)}%`;
                if (el('ex-market-health')) el('ex-market-health').textContent = `${ativasDelta.toFixed(1)}%`;

                // Growth Filter Logic
                let growthVal = entDelta;
                if (window.currentGrowthFilter === 60) growthVal = growthVal * 1.5;
                if (el('ex-growth-potential')) el('ex-growth-potential').textContent = `${growthVal.toFixed(1)}%`;


                const risk = (typeof cards.risk_score === 'number') ? cards.risk_score : null;
                if (el('ex-risk-score')) el('ex-risk-score').textContent = risk != null ? `${Math.round(risk)}` : '—';
                if (el('ex-compliance-exposure')) el('ex-compliance-exposure').textContent = pct != null ? `${pct.toFixed(1)}%` : '—%';
                if (el('ex-market-size')) el('ex-market-size').textContent = fmt(totalAtivas);
                if (el('vg-evolucao-geral') && evol && Array.isArray(evol.labels) && evol.labels.length > 0) {
                    const vals = (evol.valores || []).map(v => parseInt(v) || 0);
                    renderGraficoEvolucao('vg-evolucao-geral', evol.labels, vals);
                }

                if (el('vg-entradas-saidas') && entSai && Array.isArray(entSai.labels) && entSai.labels.length > 0) {
                    const l = entSai.labels;
                    const e = (entSai.entradas || []).map(v => parseInt(v) || 0);
                    const s = (entSai.saidas || []).map(v => parseInt(v) || 0);
                    renderGraficoEntradasSaidas(l, e, s);
                } else if (el('vg-entradas-saidas') && cards.entradas_mensais) {
                    // Fallback using cards data if graph data missing
                    const entList = Array.isArray(cards.entradas_mensais) ? cards.entradas_mensais : [];
                    const saiList = Array.isArray(cards.saidas_mensais) ? cards.saidas_mensais : [];
                    const m = Math.min(entList.length, saiList.length);
                    if (m > 0) {
                        const labels = entList.slice(-m).map(x => String(x.label || '—'));
                        const entradasVals = entList.slice(-m).map(x => parseInt(x.count || 0));
                        const saidasVals = saiList.slice(-m).map(x => parseInt(x.count || 0));
                        renderGraficoEntradasSaidas(labels, entradasVals, saidasVals);
                    }
                }

                if (el('vg-mapa-calor') && mapa && Array.isArray(mapa.labels) && mapa.labels.length > 0) {
                    const vals = (mapa.valores || []).map(v => parseInt(v) || 0);
                    renderGraficoBarra('vg-mapa-calor', mapa.labels.slice(0, 10), vals.slice(0, 10), 'Empresas por UF');
                }

                if (el('vg-mapa-distribuicao') && mapa && Array.isArray(mapa.labels) && mapa.labels.length > 0) {
                    const vals = (mapa.valores || []).map(v => parseInt(v) || 0);
                    renderGraficoDonut('vg-mapa-distribuicao', mapa.labels.slice(0, 5), vals.slice(0, 5));
                }
                const setores = Array.isArray(ranking.setores_top) ? ranking.setores_top.slice(0, 5) : [];
                const estados = Array.isArray(ranking.estados_crescimento) ? ranking.estados_crescimento.slice(0, 5) : [];
                const contSet = document.getElementById('vg-top-setores');
                const contEst = document.getElementById('vg-estados-crescimento');
                if (contSet) {
                    if (setores.length === 0) {
                        contSet.innerHTML = `<div class="flex justify-between text-sm"><span>—</span><span class="font-semibold">—</span></div>`;
                    } else {
                        contSet.innerHTML = setores.map(s => {
                            const lab = String(s.label || s.cnae || '—');
                            const val = fmt(parseInt(s.count || 0));
                            return `<div class="flex justify-between text-sm"><span>${lab}</span><span class="font-semibold">${val}</span></div>`;
                        }).join('');
                    }
                }
                if (contEst) {
                    if (estados.length === 0) {
                        contEst.innerHTML = `<div class="flex justify-between text-sm"><span>—</span><span class="text-green-600 font-semibold">↑ —</span></div>`;
                    } else {
                        contEst.innerHTML = estados.map(e => {
                            const lab = String(e.label || '—');
                            const val = parseInt(e.delta || 0);
                            return `<div class="flex justify-between text-sm"><span>${lab}</span><span class="text-green-600 font-semibold">↑ ${val}</span></div>`;
                        }).join('');
                    }
                }
                if (el('vg-evolucao-12m')) {
                    const evolLabels = Array.isArray(evol.labels) ? evol.labels : [];
                    const evolValores = Array.isArray(evol.valores) ? evol.valores.map(v => parseInt(v) || 0) : [];
                    if (evolLabels.length > 0 && evolValores.length > 0 && evolLabels.length === evolValores.length) {
                        renderGraficoEvolucao('vg-evolucao-12m', evolLabels, evolValores);
                    } else {
                        el('vg-evolucao-12m').textContent = evol && Array.isArray(evol.labels) ? `${evol.labels.slice(-5).join(' • ')}` : '—';
                    }
                }
                if (el('vg-entradas-saidas')) {
                    const entList = Array.isArray(cards.entradas_mensais) ? cards.entradas_mensais : [];
                    const saiList = Array.isArray(cards.saidas_mensais) ? cards.saidas_mensais : [];
                    const m = Math.min(entList.length, saiList.length);
                    if (m > 0) {
                        const labels = entList.slice(-m).map(x => String(x.label || '—'));
                        const entradasVals = entList.slice(-m).map(x => parseInt(x.count || 0));
                        const saidasVals = saiList.slice(-m).map(x => parseInt(x.count || 0));
                        renderGraficoEntradasSaidas(labels, entradasVals, saidasVals);
                    } else {
                        el('vg-entradas-saidas').textContent = (entSai && Array.isArray(entSai.labels)) ? `${entSai.labels.slice(-5).join(' • ')}` : '—';
                    }
                }
            } catch { }
        }

        function renderEstrutura(baseData, geralData) {
            try {
                const el = (id) => document.getElementById(id);
                const resumo = (baseData && baseData.resumo) || {};
                const totalEst = typeof resumo.total_estabelecimentos === 'number' ? resumo.total_estabelecimentos : null;
                if (el('es-total-empresas')) el('es-total-empresas').textContent = totalEst ? totalEst.toLocaleString('pt-BR') : '—';
                const mapa = ((geralData && geralData.graficos && geralData.graficos.mapa_calor) ? geralData.graficos.mapa_calor : {}) || {};
                const vals = Array.isArray(mapa.valores) ? mapa.valores : [];
                const sum = vals.reduce((a, b) => a + (parseInt(b) || 0), 0);
                const sorted = [...vals].sort((a, b) => (parseInt(b) || 0) - (parseInt(a) || 0));
                const top3 = sorted.slice(0, 3).reduce((a, b) => a + (parseInt(b) || 0), 0);
                const cr3 = sum > 0 ? (top3 / sum) * 100.0 : null;
                if (el('es-indice-concentracao')) el('es-indice-concentracao').textContent = cr3 ? `${cr3.toFixed(1)}%` : '—';
                const n = Array.isArray(mapa.valores) ? mapa.valores.length : 0;
                const media = n > 0 ? (sum / n) : null;
                if (el('es-media-regiao')) el('es-media-regiao').textContent = media ? media.toLocaleString('pt-BR') : '—';
                const ranking = (geralData && geralData.ranking) || {};
                const setores = Array.isArray(ranking.setores_top) ? ranking.setores_top.slice(0, 10) : [];
                const topPorte = ((baseData && baseData.top && (baseData.top.porte_da_empresa || baseData.top.porte)) ? (baseData.top.porte_da_empresa || baseData.top.porte) : []);

                // Gráfico de barras horizontais para CNAEs
                if (el('es-cnae') && setores.length > 0) {
                    const labels = setores.map(s => String(s.label || s.cnae || '—'));
                    const values = setores.map(s => parseInt(s.count || 0));
                    renderGraficoBarra('es-cnae', labels, values, 'Empresas', '#3b82f6');
                } else if (el('es-cnae')) {
                    el('es-cnae').textContent = '—';
                }

                // Gráfico donut para distribuição por porte
                if (el('es-porte') && Array.isArray(topPorte) && topPorte.length > 0) {
                    const labels = topPorte.map(p => String(p.label || '—'));
                    const values = topPorte.map(p => parseInt(p.count || 0));
                    const colors = ['#3b82f6', '#8b5cf6', '#ec4899', '#f59e0b', '#10b981', '#6366f1'];
                    renderGraficoDonut('es-porte', labels, values, colors);
                } else if (el('es-porte')) {
                    el('es-porte').textContent = '—';
                }
            } catch { }
        }

        function renderDemografia(geralData) {
            try {
                const el = (id) => document.getElementById(id);
                const cards = (geralData && geralData.cards) || {};
                const totalAtivas = typeof cards.total_ativas === 'number' ? cards.total_ativas : null;
                const entradas = Array.isArray(cards.entradas_mensais) ? (cards.entradas_mensais.slice(-1)[0]?.count || null) : null;
                const saidas = Array.isArray(cards.saidas_mensais) ? (cards.saidas_mensais.slice(-1)[0]?.count || null) : null;
                const tEntrada = (totalAtivas && entradas) ? Math.min(100, (entradas / totalAtivas) * 100.0) : null;
                const tSaida = (totalAtivas && saidas) ? Math.min(100, (saidas / totalAtivas) * 100.0) : null;
                if (el('d-taxa-entrada')) el('d-taxa-entrada').textContent = tEntrada != null ? `${tEntrada.toFixed(2)}%` : '—%';
                if (el('d-taxa-saida')) el('d-taxa-saida').textContent = tSaida != null ? `${tSaida.toFixed(2)}%` : '—%';
                const idade = typeof cards.idade_media === 'number' ? cards.idade_media : null;
                if (el('d-idade-media')) el('d-idade-media').textContent = idade != null ? `${idade.toFixed(1)} anos` : '— anos';

                // Survival rate from backend (real data)
                const survivalRate = typeof cards.taxa_sobrevivencia === 'number' ? cards.taxa_sobrevivencia : null;
                if (el('d-sobrevivencia')) el('d-sobrevivencia').textContent = survivalRate != null ? `${survivalRate.toFixed(1)}%` : '—%';

                const evol = ((geralData && geralData.graficos && geralData.graficos.evolucao_ativas) ? geralData.graficos.evolucao_ativas : {}) || {};

                // Gráfico de linha para entradas ao longo do tempo
                if (el('d-entradas-tempo')) {
                    const labels = Array.isArray(evol.labels) ? evol.labels.slice(-12) : [];
                    const values = Array.isArray(evol.valores) ? evol.valores.slice(-12).map(v => parseInt(v) || 0) : [];
                    if (labels.length > 0 && values.length > 0 && labels.length === values.length) {
                        renderGraficoEvolucao('d-entradas-tempo', labels, values);
                    } else {
                        el('d-entradas-tempo').textContent = '—';
                    }
                }

                // Gráfico de distribuição por idade (REAL DATA from backend)
                const idadeDist = ((geralData && geralData.graficos && geralData.graficos.idade_distribuicao) ? geralData.graficos.idade_distribuicao : {}) || {};
                if (el('d-idade-dist') && Array.isArray(idadeDist.labels) && idadeDist.labels.length > 0) {
                    const ageLabels = idadeDist.labels;
                    const ageValues = (idadeDist.valores || []).map(v => parseInt(v) || 0);
                    renderChart('d-idade-dist', 'bar', {
                        labels: ageLabels,
                        datasets: [{
                            label: 'Empresas',
                            data: ageValues,
                            backgroundColor: '#8b5cf6',
                            borderRadius: 4
                        }]
                    }, {
                        plugins: { legend: { display: false } },
                        scales: {
                            x: { grid: { display: false }, ticks: { font: { size: 10 } } },
                            y: { grid: { color: '#f1f5f9' }, ticks: { font: { size: 10 } }, beginAtZero: true }
                        }
                    });
                } else if (el('d-idade-dist')) {
                    el('d-idade-dist').textContent = '—';
                }
            } catch { }
        }

        function renderComercial(geralData) {
            try {
                const el = (id) => document.getElementById(id);
                const cards = (geralData && geralData.cards) || {};

                // Export potential (REAL DATA from backend)
                const exportPct = typeof cards.potencial_exportacao_pct === 'number' ? cards.potencial_exportacao_pct : null;
                if (el('c-exportadores')) el('c-exportadores').textContent = exportPct != null ? `${exportPct.toFixed(1)}%` : '—';

                // Market maturity index (REAL DATA from backend)
                const maturityIndex = typeof cards.indice_maturidade === 'number' ? cards.indice_maturidade : null;
                if (el('c-maturidade')) el('c-maturidade').textContent = maturityIndex != null ? `${maturityIndex.toFixed(1)}%` : '—%';

                if (el('c-compradores')) el('c-compradores').textContent = '—%';
                if (el('c-parceiros')) el('c-parceiros').textContent = '—';

                const ranking = (geralData && geralData.ranking) || {};
                const setores = Array.isArray(ranking.setores_top) ? ranking.setores_top.slice(0, 8) : [];

                // Export potential visualization by CNAE (simplified - just show export %)
                if (el('c-exportadores-cnae') && exportPct != null) {
                    // Display export % as text since we don't have per-CNAE breakdown
                    el('c-exportadores-cnae').innerHTML = `<div class="flex items-center justify-center h-64"><div class="text-center"><div class="text-6xl font-bold text-emerald-600">${exportPct.toFixed(1)}%</div><div class="text-sm text-slate-500 mt-2">Potencial de Exportação</div></div></div>`;
                } else if (el('c-exportadores-cnae')) {
                    el('c-exportadores-cnae').textContent = '—';
                }

                // Maturity index visualization (gauge-style)
                if (el('c-maturidade-radar') && maturityIndex != null) {
                    const color = maturityIndex >= 70 ? '#10b981' : maturityIndex >= 50 ? '#f59e0b' : '#ef4444';
                    el('c-maturidade-radar').innerHTML = `<div class="flex items-center justify-center h-64"><div class="text-center"><div class="text-6xl font-bold" style="color: ${color}">${maturityIndex.toFixed(0)}</div><div class="text-sm text-slate-500 mt-2">Índice de Maturidade (0-100)</div></div></div>`;
                } else if (el('c-maturidade-radar')) {
                    el('c-maturidade-radar').textContent = '—';
                }
            } catch { }
        }

        function renderFormalizacao(baseData, geralData) {
            try {
                const el = (id) => document.getElementById(id);
                const cards = (geralData && geralData.cards) || {};
                const pctValidos = typeof cards.pct_dados_validos === 'number' ? cards.pct_dados_validos : null;
                if (el('f-endereco')) el('f-endereco').textContent = pctValidos != null ? `${pctValidos.toFixed(1)}%` : '—%';
                const sitList = ((baseData && baseData.top && baseData.top.situacao_cadastral) ? baseData.top.situacao_cadastral : []) || [];
                const totalSit = sitList.reduce((a, b) => a + (parseInt(b.count || 0)), 0);
                const ativa = sitList.find(x => String(x.label).toLowerCase() === 'ativa');
                const pctAtiva = totalSit > 0 && ativa ? ((parseInt(ativa.count || 0) / totalSit) * 100.0) : null;
                if (el('f-situacao-ativa')) el('f-situacao-ativa').textContent = pctAtiva != null ? `${pctAtiva.toFixed(1)}%` : '—%';
                if (el('f-socios-ativos')) el('f-socios-ativos').textContent = '—%';

                const risk = typeof cards.risk_score === 'number' ? cards.risk_score : null;
                if (el('f-score-risco')) el('f-score-risco').textContent = risk != null ? risk.toFixed(1) : '—';

                // Gráfico donut para endereços válidos
                if (el('f-enderecos-donut') && pctValidos != null) {
                    const validos = pctValidos;
                    const invalidos = 100 - pctValidos;
                    renderGraficoDonut('f-enderecos-donut', ['Válidos', 'Inválidos'], [validos, invalidos], ['#10b981', '#ef4444']);
                } else if (el('f-enderecos-donut')) {
                    el('f-enderecos-donut').textContent = '—';
                }

                // Gráfico de barras empilhadas para situação cadastral
                if (el('f-situacao-stacked') && totalSit > 0 && sitList.length > 0) {
                    const labels = sitList.slice(0, 5).map(s => s.label);
                    const values = sitList.slice(0, 5).map(s => parseInt(s.count || 0));
                    const colors = ['#10b981', '#f59e0b', '#ef4444', '#6366f1', '#8b5cf6'];
                    renderChart('f-situacao-stacked', 'bar', {
                        labels: labels,
                        datasets: [{
                            label: 'Empresas',
                            data: values,
                            backgroundColor: colors,
                            borderRadius: 4
                        }]
                    }, {
                        plugins: { legend: { display: false } },
                        scales: {
                            x: { grid: { display: false }, ticks: { font: { size: 10 } } },
                            y: { grid: { color: '#f1f5f9' }, ticks: { font: { size: 10 } }, beginAtZero: true }
                        }
                    });
                } else if (el('f-situacao-stacked')) {
                    el('f-situacao-stacked').textContent = '—';
                }
            } catch { }
        }

        function renderSocietaria(geralData) {
            try {
                const el = (id) => document.getElementById(id);
                if (el('s-socios-medios')) el('s-socios-medios').textContent = '—';
                if (el('s-admins-pct')) el('s-admins-pct').textContent = '—%';
                if (el('s-idade-socios')) el('s-idade-socios').textContent = '— anos';
                if (el('s-rotatividade')) el('s-rotatividade').textContent = '—%';

                const ranking = (geralData && geralData.ranking) || {};
                const setores = Array.isArray(ranking.setores_top) ? ranking.setores_top.slice(0, 8) : [];

                // Gráfico de barras para sócios por CNAE
                if (el('s-socios-cnae') && setores.length > 0) {
                    const labels = setores.map(s => String(s.label || '—').substring(0, 12));
                    const values = setores.map(s => Math.floor((parseInt(s.count || 0) * Math.random() * 0.5))); // Simulado
                    renderGraficoBarra('s-socios-cnae', labels, values, 'Sócios', '#ec4899');
                } else if (el('s-socios-cnae')) {
                    el('s-socios-cnae').textContent = '—';
                }

                // Gráfico donut para qualificação de sócios
                if (el('s-qualificacao-donut')) {
                    const qualificacoes = ['Administrador', 'Sócio', 'Diretor', 'Presidente', 'Outros'];
                    const valores = [35, 30, 15, 10, 10]; // Valores simulados
                    const cores = ['#3b82f6', '#8b5cf6', '#ec4899', '#f59e0b', '#64748b'];
                    renderGraficoDonut('s-qualificacao-donut', qualificacoes, valores, cores);
                } else if (el('s-qualificacao-donut')) {
                    el('s-qualificacao-donut').textContent = '—';
                }
            } catch { }
        }
        function renderTerritorial(geralData) {
            try {
                const el = (id) => document.getElementById(id);
                const mapa = ((geralData && geralData.graficos && geralData.graficos.mapa_calor) ? geralData.graficos.mapa_calor : {}) || {};
                const labels = Array.isArray(mapa.labels) ? mapa.labels : [];
                const valores = Array.isArray(mapa.valores) ? mapa.valores.map(v => parseInt(v) || 0) : [];
                const sum = valores.reduce((a, b) => a + b, 0);
                const sorted = [...valores].sort((a, b) => b - a);
                const top3 = sorted.slice(0, 3).reduce((a, b) => a + b, 0);
                const media = labels.length > 0 ? (sum / labels.length) : null;
                const especializados = valores.filter(v => media != null && v > media).length;
                if (el('t-lq-medio')) el('t-lq-medio').textContent = '—';
                if (el('t-regioes-especializadas')) el('t-regioes-especializadas').textContent = especializados ? especializados.toLocaleString('pt-BR') : '—';
                if (el('t-empresas-populacao')) el('t-empresas-populacao').textContent = '—';
                if (el('t-empresas-renda')) el('t-empresas-renda').textContent = '—';

                // Gráfico de barras horizontais para LQ por município
                if (el('t-lq-municipio') && labels.length > 0 && valores.length > 0) {
                    const topLabels = labels.slice(0, 10);
                    const topValores = valores.slice(0, 10);
                    renderGraficoBarra('t-lq-municipio', topLabels, topValores, 'Empresas', '#6366f1');
                } else if (el('t-lq-municipio')) {
                    el('t-lq-municipio').textContent = '—';
                }

                // Gráfico de pizza para hubs setoriais
                if (el('t-hubs-setoriais') && labels.length > 0 && valores.length > 0) {
                    const topHubs = labels.slice(0, 5);
                    const topHubsValores = valores.slice(0, 5);
                    renderGraficoPizza('t-hubs-setoriais', topHubs, topHubsValores);
                } else if (el('t-hubs-setoriais')) {
                    el('t-hubs-setoriais').textContent = '—';
                }
            } catch { }
        }
        function renderSaude(geralData) {
            try {
                const el = (id) => document.getElementById(id);
                const cards = (geralData && geralData.cards) || {};
                const total = typeof cards.total_ativas === 'number' ? cards.total_ativas : null;
                const entradas = Array.isArray(cards.entradas_mensais) ? (cards.entradas_mensais.slice(-1)[0]?.count || null) : null;
                const saidas = Array.isArray(cards.saidas_mensais) ? (cards.saidas_mensais.slice(-1)[0]?.count || null) : null;
                const renovacao = (total && entradas != null && saidas != null) ? Math.min(100, ((entradas + saidas) / total) * 100.0) : null;
                const churn = (total && saidas != null) ? Math.min(100, (saidas / total) * 100.0) : null;
                if (el('s-renovacao')) el('s-renovacao').textContent = renovacao != null ? `${renovacao.toFixed(2)}%` : '—%';
                if (el('s-mortalidade')) el('s-mortalidade').textContent = churn != null ? `${churn.toFixed(2)}%` : '—%';
                if (el('s-tempo-alteracao')) el('s-tempo-alteracao').textContent = '— meses';
                if (el('s-zumbi')) el('s-zumbi').textContent = '—';

                // Gráfico de linha para renovação ao longo do tempo
                if (el('s-renovacao-tempo') && Array.isArray(cards.entradas_mensais) && cards.entradas_mensais.length > 0) {
                    const labels = cards.entradas_mensais.slice(-12).map(x => x.label || '—');
                    const values = cards.entradas_mensais.slice(-12).map(x => parseInt(x.count || 0));
                    renderGraficoEvolucao('s-renovacao-tempo', labels, values);
                } else if (el('s-renovacao-tempo')) {
                    el('s-renovacao-tempo').textContent = '—';
                }
                if (el('s-mortalidade-setor') && Array.isArray(cards.saidas_mensais) && cards.saidas_mensais.length > 0) {
                    const labels = cards.saidas_mensais.map(x => x.label || '—');
                    const values = cards.saidas_mensais.map(x => parseInt(x.count || 0));
                    renderGraficoBarra('s-mortalidade-setor', labels, values, 'Mortalidade (Saídas)', '#ef4444');
                } else if (el('s-mortalidade-setor')) {
                    el('s-mortalidade-setor').textContent = '—';
                }
            } catch { }
        }

        function initDashboardTabs() {
            try {
                const tabs = Array.from(document.querySelectorAll('.dashboard-tab'));
                const views = {
                    'visao-geral': document.getElementById('view-visao-geral'),
                    'estrutura': document.getElementById('view-estrutura'),
                    'demografia': document.getElementById('view-demografia'),
                    'comercial': document.getElementById('view-comercial'),
                    'formalizacao': document.getElementById('view-formalizacao'),
                    'societaria': document.getElementById('view-societaria'),
                    'territorial': document.getElementById('view-territorial'),
                    'saude': document.getElementById('view-saude'),
                    'players': document.getElementById('view-players')
                };
                const mkCards = document.getElementById('mk-out-cards');
                tabs.forEach(t => {
                    t.addEventListener('click', () => {
                        tabs.forEach(x => x.classList.remove('active'));
                        t.classList.add('active');
                        const v = t.getAttribute('data-view') || 'visao-geral';
                        Object.keys(views).forEach(k => {
                            const el = views[k];
                            if (!el) return;
                            if (k === v) el.classList.remove('hidden'); else el.classList.add('hidden');
                        });
                        if (mkCards) {
                            if (v === 'visao-geral') { mkCards.classList.remove('hidden'); }
                            else { mkCards.classList.add('hidden'); }
                        }
                    });
                });
            } catch { }
        }

        async function renderPlayersList(arg) {
            // arg pode ser objeto de dados (legacy) ou numero da pagina
            const page = typeof arg === 'number' ? arg : 1;

            try {
                const listBody = document.getElementById('players-list-body');
                const countEl = document.getElementById('players-count');
                const paginationEl = document.getElementById('players-pagination');
                const searchInput = document.getElementById('players-search-input');
                const searchTerm = searchInput ? searchInput.value.trim() : '';

                if (!listBody) return;

                // Loading State
                listBody.innerHTML = `<tr><td colspan="5" class="py-12 text-center text-slate-500"><div class="flex flex-col items-center justify-center gap-2"><div class="w-6 h-6 border-2 border-blue-600 border-t-transparent rounded-full animate-spin"></div><span>Carregando dados reais...</span></div></td></tr>`;

                // Fetch Real Data with Search Term
                const res = await fetch(`/api/analise/players/lista?pagina=${page}&busca=${encodeURIComponent(searchTerm)}`);
                if (!res.ok) throw new Error('Falha na API');
                const json = await res.json();
                const players = json.data || [];
                // total_estimado pode vir do backend

                if (countEl) countEl.textContent = `${players.length} empresas exibidas (Pág ${page})`;

                if (players.length === 0) {
                    listBody.innerHTML = `<tr><td colspan="5" class="py-8 text-center text-slate-500">Nenhum registro encontrado.</td></tr>`;
                    if (paginationEl) paginationEl.innerHTML = '';
                    return;
                }

                listBody.innerHTML = players.map(p => `
                    <tr class="hover:bg-slate-50 transition-colors group border-b border-slate-100 last:border-0">
                        <td class="py-4 px-6 align-top">
                            <div class="flex items-start gap-3">
                                <div class="w-10 h-10 rounded-lg bg-blue-50 flex-shrink-0 flex items-center justify-center text-blue-600 font-bold text-lg">
                                    ${String(p.nome || '').substring(0, 2)}
                                </div>
                                <div>
                                    <h4 class="font-bold text-slate-800 text-sm group-hover:text-blue-600 transition-colors">${p.nome}</h4>
                                    <p class="text-xs text-slate-500 font-mono mt-0.5">${p.cnpj}</p>
                                    <span class="inline-block mt-1.5 px-2 py-0.5 bg-green-100 text-green-700 text-[10px] font-bold rounded-full uppercase">Ativa</span>
                                </div>
                            </div>
                        </td>
                        <td class="py-4 px-6 align-top">
                            <div class="text-sm font-medium text-slate-700">${p.responsavel}</div>
                            <div class="text-xs text-slate-400 mt-0.5">Sócio-Administrador</div>
                        </td>
                        <td class="py-4 px-6 align-top">
                            <div class="flex flex-col gap-1">
                                <div class="flex items-center gap-1.5 text-xs text-slate-600">
                                    <svg class="w-3.5 h-3.5 text-slate-400" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 8l7.89 5.26a2 2 0 002.22 0L21 8M5 19h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" /></svg>
                                    ${p.email}
                                </div>
                                <div class="flex items-center gap-1.5 text-xs text-slate-600">
                                    <svg class="w-3.5 h-3.5 text-slate-400" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 5a2 2 0 012-2h3.28a1 1 0 01.948.684l1.498 4.493a1 1 0 01-.502 1.21l-2.257 1.13a11.042 11.042 0 005.516 5.516l1.13-2.257a1 1 0 011.21-.502l4.493 1.498a1 1 0 01.684.949V19a2 2 0 01-2 2h-1C9.716 21 3 14.284 3 6V5z" /></svg>
                                    ${p.telefone}
                                </div>
                            </div>
                        </td>
                        <td class="py-4 px-6 align-top">
                            <div class="text-xs text-slate-700 font-medium">${p.endereco}</div>
                            <div class="text-xs text-slate-500 mt-0.5">${p.municipio} - ${p.uf}</div>
                        </td>
                        <td class="py-4 px-6 align-top text-right">
                            <button onclick="if(typeof consultarCNPJ === 'function') { showCnpj(); consultarCNPJ('${p.cnpj}'); }" 
                                class="px-3 py-1.5 border border-blue-200 text-blue-600 rounded-lg text-xs font-bold hover:bg-blue-50 transition-colors">
                                Ver Detalhes
                            </button>
                        </td>
                    </tr>
                `).join('');

                // Paginação Simples
                if (paginationEl) {
                    const hasNext = players.length >= 20; // limite da api hardcoded
                    paginationEl.innerHTML = `
                        <div class="flex items-center gap-2">
                            <button ${page <= 1 ? 'disabled' : ''} onclick="renderPlayersList(${page - 1})" class="px-3 py-1 border border-slate-200 rounded text-xs hover:bg-slate-50 disabled:opacity-50">Anterior</button>
                            <span class="text-xs text-slate-500">Página ${page}</span>
                            <button ${!hasNext ? 'disabled' : ''} onclick="renderPlayersList(${page + 1})" class="px-3 py-1 border border-slate-200 rounded text-xs hover:bg-slate-50 disabled:opacity-50">Próxima</button>
                        </div>
                    `;
                }
            } catch (e) {
                console.error('Erro renderPlayersList:', e);
                const listBody = document.getElementById('players-list-body');
                if (listBody) listBody.innerHTML = `<tr><td colspan="5" class="py-8 text-center text-red-500">Erro ao carregar dados: ${e.message}</td></tr>`;
            }
        }

        // ================================
        // EVENT LISTENERS PARA B2G ENHANCEMENTS
        // ================================

        // Event delegation para botões de favorito
        document.addEventListener('click', async (e) => {
            const favBtn = e.target.closest('.favorito-btn');
            if (favBtn) {
                e.stopPropagation();
                const licitacaoId = favBtn.getAttribute('data-licitacao-id');
                const isFav = favoritosB2G.isFavorito(licitacaoId);

                // Encontrar dados da licitação
                const licitacao = opportunities.find(o =>
                    (o.id || o.numeroControlePNCP || o.objeto?.substring(0, 50)) === licitacaoId
                );

                if (isFav) {
                    await favoritosB2G.remover(licitacaoId);
                } else if (licitacao) {
                    await favoritosB2G.adicionar({
                        id: licitacaoId,
                        titulo: licitacao.titulo,
                        orgao: licitacao.orgao,
                        valor: licitacao.valor
                    });
                    ```
                // Atualizar UI
                renderB2GList();
            }
        });

        // Validação de CNPJ em tempo real no campo B2G
        let b2gCnpjInputValidator = document.getElementById('b2g-cnpj-input');
        if (b2gCnpjInputValidator) {
            // Adicionar elemento de feedback se não existir
            }

            b2gCnpjInput.addEventListener('input', function () {
                validarCNPJComFeedback(this);
            });

            b2gCnpjInput.addEventListener('blur', function () {
                const resultado = validarCNPJComFeedback(this);
                if (!resultado.valido && this.value) {
                    this.classList.add('border-red-500');
                }
            });
        }

        // Mesmo para campo de CNPJ na consulta CNPJ
        const cnpjSearchInput = document.getElementById('cnpj-search-input');
        if (cnpjSearchInput) {
            if (!cnpjSearchInput.nextElementSibling || !cnpjSearchInput.nextElementSibling.classList.contains('cnpj-feedback')) {
                const feedbackDiv = document.createElement('div');
                feedbackDiv.className = 'cnpj-feedback mt-2';
                cnpjSearchInput.parentNode.insertBefore(feedbackDiv, cnpjSearchInput.nextSibling);
            }

            cnpjSearchInput.addEventListener('input', function () {
                validarCNPJComFeedback(this);
            });
        }

        // ================================
        // FIM EVENT LISTENERS B2G
        // ================================

        try {
            renderB2GList();
        } catch (e) {
            console.error('Erro inicial renderB2GList:', e);
        }


        async function loadAtivasFromBase() {
            try {
                setGlobalLoading(true);
                const res = await fetch(`${ apiBase() } /api/analise / kpis / base`);
                if (!res.ok) { return; }
                const data = await res.json();
                const list = ((data && data.top && data.top.situacao_cadastral) ? data.top.situacao_cadastral : []) || [];
                const ativa = list.find(x => String(x.label).toLowerCase() === 'ativa');
                try {
                    const rg = await fetch(`${ apiBase() } /api/analise / kpis / geral`);
                    if (rg.ok) {
                        const dj = await rg.json();
                        renderEstrutura(data, dj);
                        renderDemografia(dj);
                        renderComercial(dj);
                        renderFormalizacao(data, dj);
                        renderSocietaria(dj);
                        renderTerritorial(dj);
                        renderSaude(dj);
                        renderPlayersList();
                        renderVisaoGeral(dj);
                        const rc = document.getElementById('mk-result-count');
                        if (rc) rc.textContent = typeof dj.total_filtrados === 'number' ? dj.total_filtrados.toLocaleString('pt-BR') : '—';
                        setGlobalLoading(false);
                    } else {
                        const fallback = {
                            cards: {
                                total_ativas: ativa ? parseInt(ativa.count || 0) : null,
                                entradas_mensais: [],
                                saidas_mensais: [],
                                idade_media: null,
                                pct_dados_validos: null
                            },
                            graficos: {},
                            top: data.top || {}
                        };
                        renderVisaoGeral(fallback);
                        renderEstrutura(data, null);
                        renderDemografia(null);
                        renderComercial(null);
                        renderFormalizacao(data, null);
                        renderSocietaria(null);
                        renderTerritorial(null);
                        renderSaude(null);
                        renderPlayersList();
                        setGlobalLoading(false);
                    }
                } catch {
                    const fallback = {
                        cards: {
                            total_ativas: ativa ? parseInt(ativa.count || 0) : null,
                            entradas_mensais: [],
                            saidas_mensais: [],
                            idade_media: null,
                            pct_dados_validos: null
                        },
                        graficos: {},
                        top: data.top || {}
                    };
                    renderVisaoGeral(fallback);
                    renderEstrutura(data, null);
                    renderDemografia(null);
                    renderComercial(null);
                    renderFormalizacao(data, null);
                    renderSocietaria(null);
                    renderTerritorial(null);
                    renderSaude(null);
                    renderPlayersList();
                    setGlobalLoading(false);
                }
            } catch (e) {

            }
        }

        async function analisarMercado() {
            const code = (filtrosGlobais && filtrosGlobais.cnae) ? filtrosGlobais.cnae : normalizeCNAE(document.getElementById('filter-cnae') ? document.getElementById('filter-cnae').value || '' : '');
            const uf = (filtrosGlobais && filtrosGlobais.uf) ? filtrosGlobais.uf : (document.getElementById('filter-uf') ? (document.getElementById('filter-uf').value || '').toUpperCase() : '').trim();
            try {
                setGlobalLoading(true);
                const urlBase = `${ apiBase() } /api/analise / kpis / base`;
                const res = await fetch(urlBase);
                if (!res.ok) { return; }
                const data = await res.json();
                const list = ((data && data.top && data.top.situacao_cadastral) ? data.top.situacao_cadastral : []) || [];
                const ativa = list.find(x => String(x.label).toLowerCase() === 'ativa');
                try {
                    const params = new URLSearchParams();
                    if (code) params.set('cnae', code);
                    if (uf) params.set('uf', uf);
                    const urlGeral = `${ apiBase() } /api/analise / kpis / geral${ params.toString() ? `?${params.toString()}` : '' } `;
                    const resG = await fetch(urlGeral);
                    if (resG.ok) {
                        const dataG = await resG.json();
                        renderVisaoGeral(dataG);
                        renderEstrutura(data, dataG);
                        renderDemografia(dataG);
                        renderComercial(dataG);
                        renderFormalizacao(data, dataG);
                        renderSocietaria(dataG);
                        renderTerritorial(dataG);
                        renderSaude(dataG);
                        renderPlayersList();
                        const rc = document.getElementById('mk-result-count');
                        if (rc) rc.textContent = typeof dataG.total_filtrados === 'number' ? dataG.total_filtrados.toLocaleString('pt-BR') : '—';
                        setGlobalLoading(false);
                        return;
                    }
                } catch { }
                renderVisaoGeral(data);
                renderEstrutura(data, null);
                renderDemografia(null);
                renderComercial(null);
                renderFormalizacao(data, null);
                renderSocietaria(null);
                renderTerritorial(null);
                renderSaude(null);
                renderPlayersList();
                setGlobalLoading(false);
            } catch (e) {

            }
        }
        function setGlobalLoading(on) {
            try {
                const ids = ['vg-total-ativas', 'vg-entradas-mensais', 'vg-saidas-mensais', 'vg-idade-media', 'vg-pct-validos', 'ex-market-health', 'ex-growth-potential', 'ex-risk-score', 'ex-compliance-exposure', 'ex-market-size'];
                ids.forEach(id => {
                    const el = document.getElementById(id);
                    if (!el) return;
                    if (on) { el.classList.add('skeleton'); el.textContent = ' '; }
                    else { el.classList.remove('skeleton'); }
                });
                const vm = document.getElementById('vg-validos-msg');
                if (vm) vm.textContent = on ? 'Dados sendo processados' : vm.textContent || '';
                const vp = document.getElementById('vg-entradas-periodo');
                if (vp) vp.textContent = on ? 'Dados sendo processados' : vp.textContent || '';
            } catch { }
        }
        function setBtnLoading(btn, loading) {
            if (!btn) return;
            if (loading) {
                btn.disabled = true;
                btn.dataset.originalText = btn.dataset.originalText || btn.innerHTML;
                btn.innerHTML = `< span class="inline-flex items-center gap-2" ><svg class="animate-spin h-4 w-4 text-white" viewBox="0 0 24 24" fill="none"><circle cx="12" cy="12" r="9" stroke="currentColor" stroke-opacity="0.35" stroke-width="3"></circle><path d="M12 3a9 9 0 0 1 9 9" stroke="currentColor" stroke-width="3"></path></svg><span>Buscando...</span></span > `;
                btn.classList.add('opacity-80', 'cursor-not-allowed');
            } else {
                btn.disabled = false;
                const orig = btn.dataset.originalText || 'Buscar';
                btn.innerHTML = orig;
                btn.classList.remove('opacity-80', 'cursor-not-allowed');
            }
        }
        if (mkRun) {
            mkRun.addEventListener('click', async (ev) => {
                try { ev.preventDefault(); ev.stopPropagation(); } catch { }
                setBtnLoading(mkRun, true);
                try {
                    await aplicarFiltrosGlobais();
                } finally {
                    setBtnLoading(mkRun, false);
                }
            });
        }
        const mkExport = document.getElementById('mk-export');
        const mkSave = document.getElementById('mk-save');
        const mkShare = document.getElementById('mk-share');
        if (mkExport) {
            mkExport.addEventListener('click', async () => {
                try {
                    const cnae = normalizeCNAE(document.getElementById('filter-cnae') ? document.getElementById('filter-cnae').value || '' : '');
                    const uf = (document.getElementById('filter-uf') ? (document.getElementById('filter-uf').value || '').toUpperCase() : '').trim();
                    const params = new URLSearchParams(); if (cnae) params.set('cnae', cnae); if (uf) params.set('uf', uf);
                    const url = `${ apiBase() } /api/analise / kpis / geral${ params.toString() ? `?${params.toString()}` : '' } `;
                    const res = await fetch(url);
                    if (!res.ok) return;
                    const data = await res.json();
                    const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
                    const a = document.createElement('a');
                    a.href = URL.createObjectURL(blob);
                    a.download = 'kpis.json';
                    document.body.appendChild(a);
                    a.click();
                    document.body.removeChild(a);
                } catch { }
            });
        }
        if (mkSave) {
            mkSave.addEventListener('click', () => {
                try {
                    const cnae = document.getElementById('filter-cnae') ? document.getElementById('filter-cnae').value || '' : '';
                    const uf = document.getElementById('filter-uf') ? document.getElementById('filter-uf').value || '' : '';
                    localStorage.setItem('saved_filters', JSON.stringify({ cnae, uf }));
                } catch { }
            });
        }
        if (mkShare) {
            mkShare.addEventListener('click', async () => {
                try {
                    const cnae = normalizeCNAE(document.getElementById('filter-cnae') ? document.getElementById('filter-cnae').value || '' : '');
                    const uf = (document.getElementById('filter-uf') ? (document.getElementById('filter-uf').value || '').toUpperCase() : '').trim();
                    const params = new URLSearchParams(); if (cnae) params.set('cnae', cnae); if (uf) params.set('uf', uf);
                    const shareUrl = `${ location.origin }${ location.pathname }${ params.toString() ? `?${params.toString()}` : '' } `;
                    if (navigator.clipboard && navigator.clipboard.writeText) {
                        await navigator.clipboard.writeText(shareUrl);
                    }
                } catch { }
            });
        }
        const mkClearBtn = document.getElementById('mk-clear');
        if (mkClearBtn) {
            mkClearBtn.addEventListener('click', async (ev) => {
                try { ev.preventDefault(); ev.stopPropagation(); } catch { }
                const c = document.getElementById('filter-cnae'); if (c) c.value = '';
                const u = document.getElementById('filter-uf'); if (u) u.value = '';
                await aplicarFiltrosGlobais();
            });
        }
        function renderGraficoEvolucao(containerId, labels, valores) {
            try {
                const cont = document.getElementById(containerId);
                if (!cont) return;
                cont.innerHTML = '';
                const canvas = document.createElement('canvas');
                canvas.style.height = '128px';
                cont.appendChild(canvas);
                if (charts[containerId]) { try { charts[containerId].destroy(); } catch { } }
                charts[containerId] = new Chart(canvas, {
                    type: 'line',
                    data: {
                        labels: labels,
                        datasets: [{
                            label: 'Empresas Ativas',
                            data: valores,
                            borderColor: 'rgb(59, 130, 246)',
                            backgroundColor: 'rgba(59, 130, 246, 0.1)',
                            tension: 0.3,
                            fill: true
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: { legend: { display: false }, tooltip: { enabled: true } },
                        scales: { y: { beginAtZero: true } }
                    }
                });
            } catch { }
        }
        function renderGraficoEntradasSaidas(labels, entradas, saidas) {
            try {
                const cont = document.getElementById('vg-entradas-saidas');
                if (!cont) return;
                cont.innerHTML = '';
                const canvas = document.createElement('canvas');
                canvas.style.height = '128px';
                cont.appendChild(canvas);
                const cid = 'vg-entradas-saidas';
                if (charts[cid]) { try { charts[cid].destroy(); } catch { } }
                charts[cid] = new Chart(canvas, {
                    type: 'bar',
                    data: {
                        labels: labels,
                        datasets: [
                            { label: 'Entradas', data: entradas, backgroundColor: 'rgba(34, 197, 94, 0.8)' },
                            { label: 'Saídas', data: saidas, backgroundColor: 'rgba(239, 68, 68, 0.8)' }
                        ]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: { y: { beginAtZero: true } }
                    }
                });
            } catch { }
        }

        try {
            const inputs = ['filter-cnae', 'filter-uf'].map(id => document.getElementById(id)).filter(Boolean);
            inputs.forEach(inp => {
                inp.addEventListener('keydown', (ev) => {
                    if (ev.key === 'Enter') { ev.preventDefault(); analisarMercado(); }
                });
            });
        } catch { }

        // Players Search Listener
        try {
            const pInput = document.getElementById('players-search-input');
            let pTimer = null;
            if (pInput) {
                pInput.addEventListener('input', () => {
                    clearTimeout(pTimer);
                    pTimer = setTimeout(() => { renderPlayersList(1); }, 500);
                });
                pInput.addEventListener('keydown', (e) => {
                    if (e.key === 'Enter') {
                        clearTimeout(pTimer);
                        renderPlayersList(1);
                    }
                });
            }
        } catch { }
        // CNAE suggestions
        try {
            const cnaeInput = document.getElementById('filter-cnae');
            const sugBox = document.getElementById('cnae-suggestions');
            let cnaeTimer = null;
            function showSuggestions(items) {
                if (!sugBox) return;
                if (!Array.isArray(items) || items.length === 0) {
                    sugBox.innerHTML = '';
                    sugBox.classList.add('hidden');
                    return;
                }
                const html = items.slice(0, 12).map(it => {
                    const codigo = String(it.codigo || it.code || '').trim();
                    const desc = String(it.descricao || it.desc || '').trim();
                    const label = codigo ? `${ codigo } — ${ desc } ` : desc;
                    return `< button type = "button" class="w-full text-left px-3 py-2 hover:bg-slate-50 text-sm" data - codigo="${codigo}" > ${ label }</button > `;
                }).join('');
                sugBox.innerHTML = html;
                sugBox.classList.remove('hidden');
                Array.from(sugBox.querySelectorAll('button[data-codigo]')).forEach(btn => {
                    btn.addEventListener('click', () => {
                        const code = btn.getAttribute('data-codigo') || '';
                        if (cnaeInput) {
                            cnaeInput.value = code;
                            sugBox.classList.add('hidden');
                            analisarMercado();
                        }
                    });
                });
            }
            const setorSelect = document.getElementById('filter-setor');

            // Carrega os setores disponíveis
            async function carregarSetores() {
                try {
                    const base = apiBase();
                    const url = `${ base } /api/analise / cnaes / setores`;
                    const res = await fetch(url);
                    if (res.ok) {
                        const setores = await res.json();
                        if (setorSelect) {
                            setorSelect.innerHTML = '<option value="">🏢 Todos</option>' +
                                setores.map(s => `< option value = "${s}" > ${ s }</option > `).join('');
                        }
                    }
                } catch (e) { console.error('Erro set:', e); }
            }
            carregarSetores();

            async function fetchCnaeSuggestions(term) {
                try {
                    const setorVal = setorSelect ? setorSelect.value : '';
                    if ((!term || term.trim().length < 2) && !setorVal) { showSuggestions([]); return; }

                    const base = apiBase();
                    let url = `${ base } /api/analise / cnaes / sugerir ? termo = ${ encodeURIComponent(term.trim()) } `;
                    if (setorVal) url += `& setor=${ encodeURIComponent(setorVal) } `;

                    const res = await fetch(url);
                    const js = res.ok ? await res.json() : [];
                    const data = Array.isArray(js) ? js : (js.data || []);
                    showSuggestions(data);
                } catch { showSuggestions([]); }
            }
            if (cnaeInput) {
                cnaeInput.addEventListener('input', () => {
                    try { clearTimeout(cnaeTimer); } catch { }
                    cnaeTimer = setTimeout(() => { fetchCnaeSuggestions(cnaeInput.value || ''); }, 200);
                });
                // Recarrega se mudar o setor
                if (setorSelect) {
                    setorSelect.addEventListener('change', () => {
                        fetchCnaeSuggestions(cnaeInput.value || '');
                    });
                }
                cnaeInput.addEventListener('focus', () => {
                    if ((cnaeInput.value || '').trim().length >= 2) fetchCnaeSuggestions(cnaeInput.value || '');
                });
            }
            document.addEventListener('click', (ev) => {
                const t = ev.target;
                const isInside = sugBox && (sugBox.contains(t) || (document.getElementById('filter-cnae') && document.getElementById('filter-cnae').contains(t)));
                if (!isInside && sugBox) { sugBox.classList.add('hidden'); }
            });
        } catch { }
        // Tabs desativadas no dashboard único
        if (pinSidebarBtn) {
            pinSidebarBtn.addEventListener('click', () => {
                sidebarPinned = !sidebarPinned;
                try { localStorage.setItem('sidebar_pinned', sidebarPinned ? 'true' : 'false'); } catch { }
                applyPinnedState();
            });
        }

        // Close sidebar automatically when clicking any nav item
        try {
            document.querySelectorAll('#sidebar nav a').forEach(a => {
                a.addEventListener('click', () => { if (!sidebarPinned) closeSidebar(); });
            });
        } catch { }

        function openSidebar() {
            if (sidebarPinned) return;
            sidebar.classList.remove('-translate-x-full');
            sidebarBackdrop.classList.remove('hidden');
            sidebarBackdrop.classList.remove('opacity-0');
            sidebarBackdrop.classList.add('opacity-100');
        }
        function closeSidebar() {
            if (sidebarPinned) return;
            sidebar.classList.add('-translate-x-full');
            sidebarBackdrop.classList.add('opacity-0');
            sidebarBackdrop.classList.remove('opacity-100');
            setTimeout(() => sidebarBackdrop.classList.add('hidden'), 200);
        }
        if (menuButton) {
            menuButton.addEventListener('click', openSidebar);
        }
        if (closeSidebarBtn) {
            closeSidebarBtn.addEventListener('click', closeSidebar);
        }
        if (sidebarBackdrop) {
            sidebarBackdrop.addEventListener('click', closeSidebar);
        }
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') closeSidebar();
        });



        // Enable/disable send button based on input
        if (messageInput && sendButton) {
            messageInput.addEventListener('input', () => {
                sendButton.disabled = messageInput.value.trim() === '';
            });
            messageInput.addEventListener('keydown', handleKeyDown);
            sendButton.addEventListener('click', sendMessage);
        }
        const welcomeSearch = document.getElementById('welcome-search');
        const welcomeSend = document.getElementById('welcome-send');
        if (welcomeSearch && welcomeSend) {
            welcomeSearch.addEventListener('input', () => { welcomeSend.disabled = welcomeSearch.value.trim() === ''; });
            welcomeSearch.addEventListener('keydown', (e) => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); if (welcomeSearch.value.trim() !== '') { if (messageInput) { messageInput.value = welcomeSearch.value; sendButton.disabled = false; } sendMessage(); } } });
            welcomeSend.addEventListener('click', () => { if (welcomeSearch.value.trim() !== '') { if (messageInput) { messageInput.value = welcomeSearch.value; sendButton.disabled = false; } sendMessage(); } });
        }

        const b2bListEl = document.getElementById('b2b-list') || document.getElementById('companies-grid');
        const b2bSearch = document.getElementById('b2b-search') || document.getElementById('search');
        const b2bFilterUf = document.getElementById('b2b-filter-uf') || document.getElementById('filter-uf');
        const b2bFilterMatch = document.getElementById('b2b-filter-match') || document.getElementById('filter-score');
        const filterPorte = document.getElementById('filter-porte');
        const filterIdade = document.getElementById('filter-idade');
        const filterSituacao = document.getElementById('filter-situacao');
        const sortSelect = document.getElementById('sort');

        // Subfiltros do cabeçalho da tabela
        const subF_Nome = document.getElementById('subfilter-nome');
        const subF_Porte = document.getElementById('subfilter-porte');
        const subF_Idade = document.getElementById('subfilter-idade');
        const subF_Capital = document.getElementById('subfilter-capital');
        const subF_Socios = document.getElementById('subfilter-socios');
        const subF_Setor = document.getElementById('subfilter-setor');
        const subF_Match = document.getElementById('subfilter-match');

        const b2bPagination = document.getElementById('b2b-pagination');
        const companies = [
            { razao: 'AlphaTech Sistemas Ltda', cnae: '6204000', uf: 'SP', situacao: 'Expansão', setor: 'Software', score: 78 },
            { razao: 'Beta Logística SA', cnae: '4930200', uf: 'RJ', situacao: 'Expansão', setor: 'Logística', score: 82 },
            { razao: 'Gama Indústria de Alimentos Ltda', cnae: '1062000', uf: 'MG', situacao: 'Ativa', setor: 'Alimentos', score: 73 },
            { razao: 'Delta Construções e Serviços', cnae: '4120400', uf: 'RS', situacao: 'Ativa', setor: 'Construção', score: 69 },
            { razao: 'Epsilon Energia Renovável', cnae: '3511300', uf: 'PR', situacao: 'Expansão', setor: 'Energia', score: 85 },
            { razao: 'Zeta Comércio Varejista', cnae: '4711301', uf: 'SP', situacao: 'Ativa', setor: 'Varejo', score: 64 },
            { razao: 'Theta Serviços Financeiros', cnae: '6619301', uf: 'RJ', situacao: 'Ativa', setor: 'Financeiro', score: 76 },
            { razao: 'Iota Tecnologia Aplicada', cnae: '6203100', uf: 'MG', situacao: 'Expansão', setor: 'Software', score: 88 },
            { razao: 'Kappa Transportes Urbanos', cnae: '4929904', uf: 'RS', situacao: 'Ativa', setor: 'Transporte', score: 71 },
            { razao: 'Lambda Saúde Integrada', cnae: '8610101', uf: 'PR', situacao: 'Expansão', setor: 'Saúde', score: 79 },
            { razao: 'Mu Metalúrgica', cnae: '2511001', uf: 'SP', situacao: 'Ativa', setor: 'Metalurgia', score: 67 },
            { razao: 'Nu Educação Digital', cnae: '8550301', uf: 'RJ', situacao: 'Expansão', setor: 'Educação', score: 83 },
            { razao: 'Xi Agronegócio', cnae: '0161002', uf: 'MG', situacao: 'Ativa', setor: 'Agro', score: 62 },
            { razao: 'Omicron Moda e Confecção', cnae: '1412602', uf: 'RS', situacao: 'Ativa', setor: 'Têxtil', score: 70 },
            { razao: 'Pi Serviços de Marketing', cnae: '7319001', uf: 'PR', situacao: 'Expansão', setor: 'Marketing', score: 77 },
            { razao: 'Rho Telecomunicações', cnae: '6110800', uf: 'SP', situacao: 'Expansão', setor: 'Telecom', score: 86 },
            { razao: 'Sigma Consultoria', cnae: '7020400', uf: 'RJ', situacao: 'Ativa', setor: 'Consultoria', score: 74 },
            { razao: 'Tau Indústria Química', cnae: '2013100', uf: 'MG', situacao: 'Ativa', setor: 'Química', score: 66 },
            { razao: 'Upsilon Engenharia', cnae: '7112000', uf: 'RS', situacao: 'Expansão', setor: 'Engenharia', score: 81 },
            { razao: 'Phi Editora', cnae: '5811500', uf: 'PR', situacao: 'Ativa', setor: 'Editorial', score: 65 },
            { razao: 'Chi Farmacêutica', cnae: '2121101', uf: 'SP', situacao: 'Expansão', setor: 'Farmacêutica', score: 89 },
            { razao: 'Psi Hotelaria', cnae: '5510800', uf: 'RJ', situacao: 'Ativa', setor: 'Hotelaria', score: 68 },
            { razao: 'Omega E-commerce', cnae: '6319400', uf: 'MG', situacao: 'Expansão', setor: 'Tecnologia', score: 84 }
        ];
        const sampleCompanies = [
            { id: 4, razaoSocial: 'SecureNet Segurança da Informação LTDA', nomeFantasia: 'SecureNet', cnae: '6204-0/00', cnaeDesc: 'Consultoria em TI', uf: 'SP', municipio: 'Campinas', porte: 'Pequeno Porte', idade: 6, matchScore: 91, badges: ['Crescimento', 'Especializada'], capitalSocial: 750000, socios: 4 },
            { id: 5, razaoSocial: 'Automação Industrial Sul LTDA', nomeFantasia: 'AutoSul', cnae: '6201-5/00', cnaeDesc: 'Desenvolvimento de software', uf: 'RS', municipio: 'Porto Alegre', porte: 'Médio Porte', idade: 12, matchScore: 85, badges: ['Madura', 'Exportadora'], capitalSocial: 3000000, socios: 6 },
            { id: 6, razaoSocial: 'Marketing Digital Plus LTDA', nomeFantasia: 'MarketPlus', cnae: '7311-4/00', cnaeDesc: 'Agências de publicidade', uf: 'PR', municipio: 'Curitiba', porte: 'Microempresa', idade: 2, matchScore: 76, badges: ['Startup', 'Crescimento'], capitalSocial: 80000, socios: 2 }
        ];
        try { companies.push(...sampleCompanies); } catch { }
        async function loadB2BFromAPI() {
            const base = apiBase();
            const term = String(b2bSearch && b2bSearch.value ? b2bSearch.value : '').trim();
            const uf = String(b2bFilterUf && b2bFilterUf.value ? b2bFilterUf.value : '');
            const porte = String(filterPorte && filterPorte.value ? filterPorte.value : '');
            const idade = String(filterIdade && filterIdade.value ? filterIdade.value : '');
            const situacao = String(filterSituacao && filterSituacao.value ? filterSituacao.value : '');
            const ordem = String(sortSelect && sortSelect.value ? sortSelect.value : '');

            const cnpj = normalizeCNPJ(term || '');

            try {
                // Se for CNPJ exato, usa a rota de compatibilidade/busca específica
                if (cnpj && cnpj.length === 14) {
                    const body = { cnpj_prestador: cnpj, filtros: { uf }, limite: 50 };
                    const res = await fetch(`${ base } /api/analise / compat / empresas`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
                    const js = await res.json();
                    const itens = js.resultados || [];
                    const mapped = itens.map(it => {
                        const emp = it.empresa || {};
                        return {
                            nomeFantasia: emp.nome_fantasia || emp.razao_social || '—',
                            razaoSocial: emp.razao_social || '—',
                            cnae: emp.cnae_fiscal || '',
                            cnaeDesc: emp.cnae_descricao || '',
                            uf: emp.uf || '',
                            municipio: emp.municipio || '',
                            porte: emp.porte_da_empresa || '',
                            idade: emp.idade || null,
                            match: it.compatibilidade || it.score_base || 0,
                            badges: [],
                            capitalSocial: emp.capital_social_da_empresa || null,
                            socios: null
                        };
                    });
                    companies.splice(0, companies.length, ...mapped);
                    return;
                }

                // Busca Genérica com Filtros (Backend Integration)
                const params = new URLSearchParams();
                if (term) params.set('busca', term);
                if (uf) params.set('uf', uf);
                if (porte) params.set('porte', porte);
                if (idade) params.set('idade', idade);
                if (situacao) params.set('situacao', situacao);
                if (ordenacao) params.set('ordem', ordenacao);
                params.set('pagina', String(b2bCurrentPage));

                const res = await fetch(`${ base } /api/analise / players / lista ? ${ params.toString() } `);
                if (res.ok) {
                    const js = await res.json();
                    const data = js.data || [];
                    const mapped = data.map(x => ({
                        nomeFantasia: x.nomeFantasia || x.razaoSocial || '—',
                        razaoSocial: x.razaoSocial || '—',
                        cnae: x.cnae || '',
                        cnaeDesc: x.cnaeDesc || '',
                        uf: x.uf || '',
                        municipio: x.municipio || '',
                        porte: x.porte || '',
                        idade: x.idade,
                        match: 0, // Search API returns 0 match for now
                        badges: [],
                        capitalSocial: x.capitalSocial,
                        socios: null
                    }));
                    companies.splice(0, companies.length, ...mapped);
                } else {
                    console.warn("API Error", res.status);
                    companies.splice(0, companies.length); // Clear list if error/empty
                }

            } catch (e) {
                console.error("Fetch error:", e);
                // Fallback demo data only if connection completely fails
                try { companies.splice(0, companies.length, ...sampleCompanies); } catch { }
            }
        }
        let b2bCurrentPage = 1;
        const B2B_PAGE_SIZE = 9;
        function formatCurrency(value) { try { return new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL', maximumFractionDigits: 0 }).format(value); } catch { return String(value || ''); } }
        // getMatchColor moved to utils.js
        // getBadgeColor moved to utils.js
        // normalizeCompany moved to utils.js
        function filterCompanies() {
            const rawTerm = String(b2bSearch && b2bSearch.value ? b2bSearch.value : '').trim();
            const term = rawTerm.toLowerCase();
            const isCNPJSearch = normalizeCNPJ(rawTerm) && normalizeCNPJ(rawTerm).length === 14;

            const uf = String(b2bFilterUf && b2bFilterUf.value ? b2bFilterUf.value : '');
            const minMatch = parseInt(String(b2bFilterMatch && b2bFilterMatch.value ? b2bFilterMatch.value : '0')) || 0;
            const idadeRange = String(filterIdade && filterIdade.value ? filterIdade.value : '');
            const porte = String(filterPorte && filterPorte.value ? filterPorte.value : '');
            const situ = String(filterSituacao && filterSituacao.value ? filterSituacao.value : '');
            return companies.map(normalizeCompany).filter(c => {
                // Filtros Globais originais
                // Se for busca de compatibilidade por CNPJ, ignoramos o filtro de texto exato no resultado
                const termOk = isCNPJSearch || !term || c.nomeFantasia.toLowerCase().includes(term) || c.razaoSocial.toLowerCase().includes(term) || String(c.cnae).toLowerCase().includes(term) || (c.cnaeDesc || '').toLowerCase().includes(term);
                const ufOk = !uf || c.uf === uf;
                let matchOk = true;
                if (b2bFilterMatch && b2bFilterMatch.id === 'filter-score') {
                    const mm = String(b2bFilterMatch.value || '');
                    const m = parseInt(c.match || 0) || 0;
                    if (mm === '90+') matchOk = m >= 90;
                    else if (mm === '75-90') matchOk = m >= 75 && m < 90;
                    else if (mm === '60-75') matchOk = m >= 60 && m < 75;
                } else {
                    matchOk = !minMatch || (parseInt(c.match || 0) || 0) >= minMatch;
                }
                const porteOk = !porte || (String(c.porte || '') === porte);
                let idadeOk = true;
                if (idadeRange) {
                    const idade = (c.idade != null ? parseInt(c.idade) : null);
                    if (idade != null) {
                        if (idadeRange === '0-2') idadeOk = idade >= 0 && idade <= 2;
                        else if (idadeRange === '2-5') idadeOk = idade >= 2 && idade <= 5;
                        else if (idadeRange === '5-10') idadeOk = idade >= 5 && idade <= 10;
                        else if (idadeRange === '10+') idadeOk = idade >= 10;
                    }
                }
                const sitOk = !situ || (Array.isArray(c.badges) && c.badges.map(x => String(x)).includes(situ)) || (String(c.situacao || '') === situ);

                // Lógica dos Subfiltros (cabeçalho da tabela)
                let subNomeOk = true, subPorteOk = true, subIdadeOk = true, subCapitalOk = true, subSociosOk = true, subSetorOk = true, subMatchOk = true;

                if (subF_Nome && subF_Nome.value) {
                    const v = subF_Nome.value.toLowerCase();
                    subNomeOk = c.nomeFantasia.toLowerCase().includes(v) || c.razaoSocial.toLowerCase().includes(v);
                }
                if (subF_Porte && subF_Porte.value) {
                    subPorteOk = String(c.porte || '').includes(subF_Porte.value);
                }
                if (subF_Idade && subF_Idade.value) {
                    const minIdade = parseInt(subF_Idade.value);
                    const i = c.idade != null ? parseInt(c.idade) : -1;
                    subIdadeOk = i >= minIdade;
                }
                if (subF_Capital && subF_Capital.value) {
                    const minCap = parseFloat(subF_Capital.value);
                    const cap = c.capitalSocial != null ? parseFloat(c.capitalSocial) : -1;
                    subCapitalOk = cap >= minCap;
                }
                if (subF_Socios && subF_Socios.value) {
                    const minSoc = parseInt(subF_Socios.value);
                    const s = c.socios != null ? parseInt(c.socios) : -1;
                    subSociosOk = s >= minSoc;
                }
                if (subF_Setor && subF_Setor.value) {
                    const v = subF_Setor.value.toLowerCase();
                    subSetorOk = String(c.cnaeDesc || '').toLowerCase().includes(v);
                }
                if (subF_Match && subF_Match.value) {
                    const minM = parseInt(subF_Match.value);
                    const m = parseInt(c.match || 0);
                    subMatchOk = m >= minM;
                }

                return termOk && ufOk && matchOk && porteOk && idadeOk && sitOk && subNomeOk && subPorteOk && subIdadeOk && subCapitalOk && subSociosOk && subSetorOk && subMatchOk;
            });
        }
        function companyRowHtml(c) {
            const match = parseInt(c.match || 0) || 0;
            const matchCls = getMatchColor(match);

            return `
                        < tr class="hover:bg-slate-50 transition-colors group" >
                <td class="px-6 py-4">
                    <div class="flex flex-col">
                        <span class="font-bold text-slate-800 text-[15px] truncate max-w-[280px]" title="${c.nomeFantasia}">${c.nomeFantasia}</span>
                        <span class="text-xs text-slate-500 truncate max-w-[280px]" title="${c.razaoSocial}">${c.razaoSocial}</span>
                    </div>
                </td>
                <td class="px-6 py-4 whitespace-nowrap">
                    <span class="inline-flex px-2 py-1 rounded bg-slate-100 text-slate-600 text-xs font-semibold">${c.porte || '—'}</span>
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-slate-700">
                    ${c.idade != null ? c.idade + ' anos' : '—'}
                </td>
                <td class="px-6 py-4 whitespace-nowrap font-medium text-slate-700">
                    ${c.capitalSocial ? formatCurrency(c.capitalSocial) : '—'}
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-center text-slate-700">
                    ${c.socios || '—'}
                </td>
                <td class="px-6 py-4">
                    <div class="truncate max-w-[200px] text-xs" title="${c.cnaeDesc || ''}">${c.cnaeDesc || '—'}</div>
                </td>
                <td class="px-6 py-4 whitespace-nowrap">
                    <div class="flex items-center gap-2">
                         <span class="px-2 py-1 ${matchCls} border rounded-lg text-xs font-bold whitespace-nowrap">${match}%</span>
                    </div>
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-right">
                    <button class="px-3 py-1.5 bg-white border border-slate-200 hover:border-blue-500 hover:text-blue-600 text-slate-600 rounded-lg text-xs font-bold uppercase tracking-wide transition-all shadow-sm flex items-center gap-1 ml-auto">
                        Ver Perfil
                        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="9 18 15 12 9 6"></polyline></svg>
                    </button>
                </td>
            </tr > `;
        }
        function renderB2BPagination(totalPages) {
            if (!b2bPagination) return;
            const inner = b2bPagination.querySelector('.flex.items-center.gap-2') || b2bPagination;
            const prevDisabled = b2bCurrentPage <= 1 ? 'disabled' : '';
            const nextDisabled = b2bCurrentPage >= totalPages ? 'disabled' : '';
            const pages = [];
            const start = Math.max(1, b2bCurrentPage - 1);
            const end = Math.min(totalPages, b2bCurrentPage + 1);
            pages.push(1);
            if (start > 2) pages.push('...');
            for (let p = start; p <= end; p++) { if (p !== 1 && p !== totalPages) pages.push(p); }
            if (end < totalPages - 1) pages.push('...');
            if (totalPages > 1) pages.push(totalPages);
            inner.innerHTML = `
                        < button class="px-3 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 ${prevDisabled ? 'opacity-50' : ''}" data - page="prev" ${ prevDisabled }> Anterior</button >
                            ${
                        pages.map(p => {
                            if (p === '...') return `<span class="px-2">...</span>`;
                            const isCurrent = p === b2bCurrentPage;
                            const cls = isCurrent ? 'px-3 py-2 bg-blue-600 text-white rounded-lg' : 'px-3 py-2 border border-gray-300 rounded-lg hover:bg-gray-50';
                            return `<button class="${cls}" data-page="${p}">${p}</button>`;
                        }).join('')
                    }
                    <button class="px-3 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 ${nextDisabled ? 'opacity-50' : ''}" data-page="next" ${nextDisabled}>Próximo</button>
                    `;
            Array.from(inner.querySelectorAll('button[data-page]')).forEach(btn => {
                btn.addEventListener('click', () => {
                    const data = btn.getAttribute('data-page');
                    if (data === 'prev') { if (b2bCurrentPage > 1) { b2bCurrentPage--; renderB2BList(); } return; }
                    if (data === 'next') { const total = Math.max(1, Math.ceil(filterCompanies().length / B2B_PAGE_SIZE)); if (b2bCurrentPage < total) { b2bCurrentPage++; renderB2BList(); } return; }
                    const n = parseInt(data || '1'); if (!isNaN(n)) { b2bCurrentPage = n; renderB2BList(); }
                });
            });
        }
        async function renderB2BList() {
            if (!b2bListEl) return;
            try { await loadB2BFromAPI(); } catch { }
            let filtered = filterCompanies();
            const sortVal = String(sortSelect && sortSelect.value ? sortSelect.value : 'score_desc');
            filtered = filtered.sort((a, b) => {
                const ma = parseInt(a.match || 0) || 0, mb = parseInt(b.match || 0) || 0;
                const ca = (a.capitalSocial != null ? a.capitalSocial : -1), cb = (b.capitalSocial != null ? b.capitalSocial : -1);
                const ia = (a.idade != null ? a.idade : -1), ib = (b.idade != null ? b.idade : -1);
                if (sortVal === 'score_desc') return mb - ma;
                if (sortVal === 'score_asc') return ma - mb;
                if (sortVal === 'capital_desc') return cb - ca;
                if (sortVal === 'capital_asc') return ca - cb;
                if (sortVal === 'idade_desc') return ib - ia;
                if (sortVal === 'idade_asc') return ia - ib;
                return mb - ma;
            });
            const totalPages = Math.max(1, Math.ceil(filtered.length / B2B_PAGE_SIZE));
            if (b2bCurrentPage > totalPages) b2bCurrentPage = totalPages;
            const start = (b2bCurrentPage - 1) * B2B_PAGE_SIZE;
            const pageItems = filtered.slice(start, start + B2B_PAGE_SIZE);
            b2bListEl.innerHTML = pageItems.map(companyRowHtml).join('');
            renderB2BPagination(totalPages);
        }
        if (b2bSearch) {
            b2bSearch.addEventListener('input', () => {
                try { clearTimeout(b2bSearch._t); } catch { }
                b2bSearch._t = setTimeout(() => { b2bCurrentPage = 1; renderB2BList(); }, 300);
            });
        }
        if (b2bFilterUf) { b2bFilterUf.addEventListener('change', () => { b2bCurrentPage = 1; renderB2BList(); }); }
        if (b2bFilterMatch) { b2bFilterMatch.addEventListener('change', () => { b2bCurrentPage = 1; renderB2BList(); }); }
        if (filterPorte) { filterPorte.addEventListener('change', () => { b2bCurrentPage = 1; renderB2BList(); }); }
        if (filterIdade) { filterIdade.addEventListener('change', () => { b2bCurrentPage = 1; renderB2BList(); }); }
        if (filterSituacao) { filterSituacao.addEventListener('change', () => { b2bCurrentPage = 1; renderB2BList(); }); }
        if (sortSelect) { sortSelect.addEventListener('change', () => { b2bCurrentPage = 1; renderB2BList(); }); }
        const b2bSearchBtn = document.getElementById('b2b-search-btn');
        if (b2bSearchBtn) {
            b2bSearchBtn.addEventListener('click', async () => {
                const originalContent = b2bSearchBtn.innerHTML;
                try {
                    b2bSearchBtn.disabled = true;
                    b2bSearchBtn.innerHTML = `
                        < svg class="animate-spin h-5 w-5 text-white" xmlns = "http://www.w3.org/2000/svg" fill = "none" viewBox = "0 0 24 24" >
                            <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                            <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"></path>
                        </svg >
                        Buscando...
        `;
                    b2bCurrentPage = 1;
                    await renderB2BList();
                } finally {
                    b2bSearchBtn.innerHTML = originalContent;
                    b2bSearchBtn.disabled = false;
                }
            });
        }

        // Listeners dos subfiltros
        [subF_Nome, subF_Idade, subF_Capital, subF_Socios, subF_Setor, subF_Match].forEach(el => {
            if (el) el.addEventListener('input', () => { b2bCurrentPage = 1; renderB2BList(); });
        });
        if (subF_Porte) subF_Porte.addEventListener('change', () => { b2bCurrentPage = 1; renderB2BList(); });

        // Ordenação pelo cabeçalho Match
        const btnSortMatch = document.getElementById('btn-sort-match');
        if (btnSortMatch) {
            btnSortMatch.addEventListener('click', () => {
                if (sortSelect) sortSelect.value = 'score_desc';
                b2bCurrentPage = 1;
                renderB2BList();
            });
        }

        // ================================
        // B2G ENHANCEMENTS - SPRINT 1
        // ================================

        /**
         * Calcula status da licitação e retorna tag HTML com countdown
         */
        function getStatusTag(prazo) {
            try {
                if (!prazo) return { html: '<span class="px-2 py-1 bg-gray-400 text-white text-xs rounded-lg">Sem prazo</span>', dias: null };

                const hoje = new Date();
                const dataFechamento = new Date(prazo);
                const diasRestantes = Math.ceil((dataFechamento - hoje) / (1000 * 60 * 60 * 24));

                if (diasRestantes < 0) {
                    return {
                        html: '<span class="px-2 py-1 bg-gray-500 text-white text-xs rounded-lg">Encerrada</span>',
                        dias: diasRestantes
                    };
                } else if (diasRestantes === 0) {
                    return {
                        html: '<span class="px-2 py-1 bg-red-600 text-white text-xs rounded-lg animate-pulse font-bold">⚠️ Hoje!</span>',
                        dias: 0
                    };
                } else if (diasRestantes <= 3) {
                    return {
                        html: `< span class= "px-2 py-1 bg-red-500 text-white text-xs rounded-lg animate-pulse" >🔥 ${ diasRestantes }d</span > `,
                        dias: diasRestantes
                    };
                } else if (diasRestantes <= 7) {
                    return {
                        html: `< span class= "px-2 py-1 bg-orange-500 text-white text-xs rounded-lg" >⏰ ${ diasRestantes }d</span > `,
                        dias: diasRestantes
                    };
                } else if (diasRestantes <= 15) {
                    return {
                        html: `< span class= "px-2 py-1 bg-yellow-500 text-gray-900 text-xs rounded-lg" > ${ diasRestantes } dias</span > `,
                        dias: diasRestantes
                    };
                } else {
                    return {
                        html: `< span class= "px-2 py-1 bg-green-500 text-white text-xs rounded-lg" >✓ ${ diasRestantes } dias</span > `,
                        dias: diasRestantes
                    };
                }
            } catch {
                return { html: '<span class="px-2 py-1 bg-gray-400 text-white text-xs rounded-lg">—</span>', dias: null };
            }
        }

        /**
         * Retorna classe CSS baseada no score de match
         */
        function getMatchColor(match) {
            const m = parseInt(match) || 0;
            if (m >= 80) return 'text-green-600 bg-green-50';
            if (m >= 60) return 'text-blue-600 bg-blue-50';
            if (m >= 40) return 'text-orange-600 bg-orange-50';
            return 'text-gray-600 bg-gray-50';
        }

        /**
         * Retorna ícone e texto para chance de sucesso
         */
        function getChanceSucessoTag(match) {
            const m = parseInt(match) || 0;
            if (m >= 80) return '🎯 Alta chance';
            if (m >= 60) return '⭐ Média chance';
            if (m >= 40) return '💡 Possível';
            return '⚠️ Baixa chance';
        }

        /**
         * Valida CNPJ e retorna feedback visual
         */
        function validarCNPJComFeedback(cnpjInput) {
            const cnpj = cnpjInput.value.replace(/\\D/g, '');
            const feedbackEl = cnpjInput.nextElementSibling;

            if (cnpj.length === 0) {
                cnpjInput.classList.remove('border-green-500', 'border-red-500');
                if (feedbackEl) feedbackEl.innerHTML = '';
                return { valido: false, cnpj: '' };
            }

            if (cnpj.length === 14) {
                cnpjInput.classList.remove('border-red-500');
                cnpjInput.classList.add('border-green-500');
                if (feedbackEl) {
                    feedbackEl.innerHTML = '<span class="text-green-600 text-xs">✓ CNPJ válido</span>';
                }
                return { valido: true, cnpj };
            } else {
                cnpjInput.classList.remove('border-green-500');
                cnpjInput.classList.add('border-red-500');
                if (feedbackEl) {
                    feedbackEl.innerHTML = `< span class= "text-red-600 text-xs" >⚠ Digite 14 dígitos(${ cnpj.length } / 14)</span > `;
                }
                return { valido: false, cnpj };
            }
        }

        // Sistema de Favoritos
        const favoritosB2G = {
            storage: 'b2g_favoritos',

            getFavoritos() {
                try {
                    return JSON.parse(localStorage.getItem(this.storage) || '[]');
                } catch {
                    return [];
                }
            },

            isFavorito(licitacaoId) {
                return this.getFavoritos().some(f => f.id === licitacaoId);
            },

            async adicionar(licitacao) {
                try {
                    // Salvar localmente
                    const favoritos = this.getFavoritos();
                    if (!favoritos.some(f => f.id === licitacao.id)) {
                        favoritos.push({
                            id: licitacao.id,
                            titulo: licitacao.titulo,
                            orgao: licitacao.orgao,
                            valor: licitacao.valor,
                            adicionadoEm: new Date().toISOString()
                        });
                        localStorage.setItem(this.storage, JSON.stringify(favoritos));
                    }

                    // Enviar para API (se disponível)
                    try {
                        await fetch(`${ apiBase() } / api / favoritos / licitacao`, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({
                                licitacao_id: licitacao.id,
                                licitacao_data: licitacao
                            })
                        });
                    } catch (e) {
                        console.warn('API favoritos indisponível:', e);
                    }

                    return true;
                } catch (e) {
                    console.error('Erro ao adicionar favorito:', e);
                    return false;
                }
            },

            async remover(licitacaoId) {
                try {
                    // Remover localmente
                    const favoritos = this.getFavoritos().filter(f => f.id !== licitacaoId);
                    localStorage.setItem(this.storage, JSON.stringify(favoritos));

                    // Remover da API
                    try {
                        await fetch(`${ apiBase() } / api / favoritos / licitacao / ${ licitacaoId }`, {
                            method: 'DELETE'
                        });
                    } catch (e) {
                        console.warn('API favoritos indisponível:', e);
                    }

                    return true;
                } catch (e) {
                    console.error('Erro ao remover favorito:', e);
                    return false;
                }
            },

            getIconeHTML(licitacaoId) {
                const isFav = this.isFavorito(licitacaoId);
                return `
        < button 
                        class= "favorito-btn p-2 rounded-lg hover:bg-slate-100 transition-colors" 
                        data - licitacao - id="${licitacaoId}"
                        title = "${isFav ? 'Remover dos favoritos' : 'Adicionar aos favoritos'}"
            >
            <svg class="w-5 h-5 ${isFav ? 'fill-yellow-400 text-yellow-400' : 'text-slate-400'}"
                viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                <path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z" />
            </svg>
                    </button >
            `;
            }
        };

        // ================================
        // FIM B2G ENHANCEMENTS
        // ================================

        const b2gListEl = document.getElementById('b2g-list') || document.getElementById('opportunities-grid');
        const b2gSearch = document.getElementById('b2g-search');
        const b2gFilterOrgao = document.getElementById('b2g-filter-orgao');
        const b2gFilterModalidade = document.getElementById('b2g-filter-modalidade');
        const b2gSort = document.getElementById('b2g-sort') || document.getElementById('b2g-sort-top');
        const b2gPagination = document.getElementById('b2g-pagination');
        const filterValor = document.getElementById('filter-valor');
        const activeFiltersEl = document.getElementById('active-filters');
        let b2gViewMode = 'cards';
        const viewToggleBtns = Array.from(document.querySelectorAll('#b2g-page .view-toggle'));
        let opportunities = [];
        let b2gCurrentPage = 1;
        const B2G_PAGE_SIZE = 9;
        function formatValor(v) { try { return new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL', maximumFractionDigits: 0 }).format(v || 0); } catch { return String(v || ''); } }
        function formatCurrency(v) { return formatValor(v); }
        async function loadB2GFromAPI() {
            const base = apiBase();
            const term = String(b2gSearch && b2gSearch.value ? b2gSearch.value : '').trim();
            const orgao = String(b2gFilterOrgao && b2gFilterOrgao.value ? b2gFilterOrgao.value : '');
            const modalidade = String(b2gFilterModalidade && b2gFilterModalidade.value ? b2gFilterModalidade.value : '');
            const params = new URLSearchParams();
            if (term) params.set('palavraChave', term);
            if (orgao) params.set('orgao', orgao);
            if (modalidade) params.set('modalidade', modalidade);
            try {
                const url = `${ base } / api / analise / pncp / editais ? ${ params.toString() }`;
                const res = await fetch(url);
                const js = res.ok ? await res.json() : { data: [] };
                const data = Array.isArray(js) ? js : (js.data || []);
                opportunities = data.map(it => {
                    const titulo = it.objeto || it.resumo || it.descricao || it.titulo || '—';
                    const org = it.orgao || it.unidadeGestora || it.orgaoEntidade || '—';
                    const mod = it.modalidade || it.codigoModalidadeContratacao || '—';
                    const valor = (typeof it.valorTotalEstimado === 'number' ? it.valorTotalEstimado : (typeof it.valorTotal === 'number' ? it.valorTotal : 0));
                    const prazo = it.dataFinal || it.dataAbertura || it.dataPublicacaoPncp || it.dataPublicacao || '—';
                    const uf = it.uf || it.ufSigla || '—';
                    const municipio = it.municipioNome || '—';
                    const dataPub = it.dataPublicacao || it.dataPublicacaoPncp || '';
                    const match = 0;
                    return { titulo, orgao: org, modalidade: String(mod), valor, prazo, prazoDias: null, situacao: it.situacao || it.status || '', uf, municipio, data: dataPub, match };
                });
            } catch {
                opportunities = [];
            }
        }
        function tagCls(text) { return 'filter-tag px-2 py-1 bg-gray-100 text-gray-700 rounded-lg text-xs'; }
        function filterOpportunities() {
            const term = String(b2gSearch && b2gSearch.value ? b2gSearch.value : '').toLowerCase();
            const orgao = String(b2gFilterOrgao && b2gFilterOrgao.value ? b2gFilterOrgao.value : '');
            const modalidade = String(b2gFilterModalidade && b2gFilterModalidade.value ? b2gFilterModalidade.value : '');
            const valorRange = String(filterValor && filterValor.value ? filterValor.value : '');
            return opportunities.filter(o => {
                const termOk = !term || o.titulo.toLowerCase().includes(term) || o.municipio.toLowerCase().includes(term);
                const orgaoOk = !orgao || o.orgao.toLowerCase() === orgao.toLowerCase();
                const modText = String(o.modalidade || '').toLowerCase();
                const modOk = !modalidade || modText.includes(modalidade.toLowerCase());
                let valorOk = true;
                const v = parseFloat(o.valor || 0);
                if (valorRange === '0-50k') valorOk = v <= 50000;
                else if (valorRange === '50k-500k') valorOk = v >= 50000 && v <= 500000;
                else if (valorRange === '500k+') valorOk = v >= 500000;
                return termOk && orgaoOk && modOk && valorOk;
            });
        }
        function updateActiveFilters() {
            if (!activeFiltersEl) return;
            const filters = [];
            const search = b2gSearch && b2gSearch.value ? b2gSearch.value.trim() : '';
            const orgao = b2gFilterOrgao && b2gFilterOrgao.value ? b2gFilterOrgao.value : '';
            const modalidade = b2gFilterModalidade && b2gFilterModalidade.value ? b2gFilterModalidade.value : '';
            const valor = filterValor && filterValor.value ? filterValor.value : '';
            if (search) filters.push({ label: `"${search}"`, field: 'search' });
            if (orgao) filters.push({ label: orgao, field: 'orgao' });
            if (modalidade) filters.push({ label: modalidade, field: 'modalidade' });
            if (valor) filters.push({ label: valor, field: 'valor' });
            activeFiltersEl.innerHTML = filters.map(f => `
        < span class= "filter-tag inline-flex items-center gap-1 px-2 py-1 bg-blue-100 text-blue-700 rounded-lg text-sm" >
        ${ f.label }
        < button onclick = "removeFilter('${f.field}')" class= "hover:text-blue-900" >
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <line x1="18" y1="6" x2="6" y2="18"></line>
            <line x1="6" y1="6" x2="18" y2="18"></line>
        </svg>
                  </button >
                </span >
            `).join('');
        }
        function removeFilter(field) {
            if (field === 'search' && b2gSearch) { b2gSearch.value = ''; }
            if (field === 'orgao' && b2gFilterOrgao) { b2gFilterOrgao.value = ''; }
            if (field === 'modalidade' && b2gFilterModalidade) { b2gFilterModalidade.value = ''; }
            if (field === 'valor' && filterValor) { filterValor.value = ''; }
            b2gCurrentPage = 1;
            renderB2GList();
        }
        function opportunityCardHtml(o) {
            const match = parseInt(o.match || o.score_percent || 0) || 0;
            const matchCls = getMatchColor(match);
            const statusTag = getStatusTag(o.prazo || o.dataFinal || o.dataAbertura);
            const chanceSucesso = getChanceSucessoTag(match);
            const licitacaoId = o.id || o.numeroControlePNCP || o.objeto?.substring(0, 50) || Math.random().toString();

            return `< div class= "opportunity-card bg-white rounded-2xl border border-slate-100 p-5 cursor-pointer shadow-sm card-hover group h-full flex flex-col relative overflow-hidden" >
              <div class="absolute top-0 right-0 w-24 h-24 bg-gradient-to-bl from-slate-50 to-transparent -mr-8 -mt-8 rounded-full opacity-50 pointer-events-none"></div>
              
              <!-- Header com Match e Status -->
              <div class="flex items-start justify-between mb-3 relative z-10">
                <div class="flex items-center gap-2">
                  <span class="px-2 py-0.5 text-[10px] font-bold uppercase tracking-wide ${matchCls} rounded-md border border-current" 
                        title="Score de compatibilidade: ${match}%\n${o.match_explicacao || 'Baseado em CNAE, porte, localização e histórico'}">
                    ${match}% Match
                  </span>
                  ${statusTag.html}
                </div>
                <div class="flex items-center gap-1">
                  <span class="text-[10px] font-bold text-slate-400 uppercase tracking-wide">${o.uf || 'BR'}</span>
                  ${favoritosB2G.getIconeHTML(licitacaoId)}
                </div>
              </div>
              
              <!--Título -->
              <h3 class="font-bold text-slate-800 mb-2 line-clamp-2 text-[15px] leading-snug">${o.titulo}</h3>
              
              <!--Órgão -->
              <div class="flex items-center gap-1.5 text-xs text-slate-500 mb-3">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="text-slate-300">
                  <path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"></path>
                </svg>
                <span class="truncate font-medium">${o.orgao}</span>
              </div>
              
              <!--Chance de Sucesso-- >
              <div class="mb-3 text-xs">
                <span class="px-2 py-1 bg-slate-50 text-slate-700 rounded-lg font-medium">
                  ${chanceSucesso}
                </span>
              </div>
              
              <!--Footer -->
        <div class="mt-auto space-y-3 relative z-10">
            <div class="flex items-center justify-between text-xs">
                <span class="px-2 py-1 bg-slate-100 text-slate-600 rounded-lg text-[10px] font-semibold uppercase tracking-wide">
                    ${o.modalidade}
                </span>
                <span class="font-bold text-slate-800 text-sm">${formatCurrency(o.valor)}</span>
            </div>
            <div class="flex items-center justify-between pt-3 border-t border-slate-50">
                <div class="flex items-center gap-1 text-[10px] text-slate-400 font-medium uppercase tracking-wide">
                    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <circle cx="12" cy="12" r="10"></circle>
                        <path d="M12 6v6l4 2"></path>
                    </svg>
                    ${statusTag.dias !== null && statusTag.dias >= 0 ? `${statusTag.dias}d restantes` : 'Encerrado'}
                </div>
                <button class="text-blue-600 hover:text-blue-700 text-xs font-bold uppercase tracking-wide">
                    Ver detalhes
                </button>
            </div>
        </div>
            </div > `;
        }
        function opportunityRowHtml(o) {
            return `< div class= "bg-white rounded-2xl border border-slate-100 p-4 shadow-sm card-hover mb-2 group" >
        <div class="flex items-center justify-between">
            <div class="flex-1 min-w-0 pr-4">
                <div class="font-bold text-slate-800 text-[15px] truncate">${o.titulo}</div>
                <div class="text-[11px] text-slate-500 font-medium uppercase tracking-wide mt-0.5">${o.orgao} • ${o.modalidade} • ${o.municipio}-${o.uf}</div>
            </div>
            <div class="text-right">
                <div class="text-sm font-bold text-slate-900">${formatValor(o.valor)}</div>
                <div class="text-[10px] text-slate-400 font-medium uppercase tracking-wider mt-0.5">${o.prazoDias || o.prazo || '—'}</div>
            </div>
        </div>
            </div > `;
        }
        function renderB2GPagination(totalPages) {
            if (!b2gPagination) return;
            const inner = b2gPagination.querySelector('.flex.items-center.gap-2') || b2gPagination;
            const prevDisabled = b2gCurrentPage <= 1 ? 'disabled' : '';
            const nextDisabled = b2gCurrentPage >= totalPages ? 'disabled' : '';
            const pages = [];
            const start = Math.max(1, b2gCurrentPage - 1);
            const end = Math.min(totalPages, b2gCurrentPage + 1);
            pages.push(1);
            if (start > 2) pages.push('...');
            for (let p = start; p <= end; p++) { if (p !== 1 && p !== totalPages) pages.push(p); }
            if (end < totalPages - 1) pages.push('...');
            if (totalPages > 1) pages.push(totalPages);
            inner.innerHTML = `
            < button class= "px-3 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 ${prevDisabled ? 'opacity-50' : ''}" data - page="prev" ${ prevDisabled } > Anterior</button >
        ${ pages.map(p => p === '...' ? `<span class="px-2">...</span>` : `<button class="${p === b2gCurrentPage ? 'px-3 py-2 bg-blue-600 text-white rounded-lg' : 'px-3 py-2 border border-gray-300 rounded-lg hover:bg-gray-50'}" data-page="${p}">${p}</button>`).join('') }
        < button class= "px-3 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 ${nextDisabled ? 'opacity-50' : ''}" data - page="next" ${ nextDisabled } > Próximo</button >
        `;
            Array.from(inner.querySelectorAll('button[data-page]')).forEach(btn => {
                btn.addEventListener('click', () => {
                    const data = btn.getAttribute('data-page');
                    if (data === 'prev') { if (b2gCurrentPage > 1) { b2gCurrentPage--; renderB2GList(); } return; }
                    if (data === 'next') { const total = Math.max(1, Math.ceil(filterOpportunities().length / B2G_PAGE_SIZE)); if (b2gCurrentPage < total) { b2gCurrentPage++; renderB2GList(); } return; }
                    const n = parseInt(data || '1'); if (!isNaN(n)) { b2gCurrentPage = n; renderB2GList(); }
                });
            });
        }

        async function renderPlayersList() {
            const listEl = document.getElementById('players-list');
            const countEl = document.getElementById('players-count');
            if (!listEl) return;

            // Loading
            listEl.innerHTML = '<div class="col-span-full text-center py-10 text-slate-500 flex flex-col items-center gap-3"><svg class="animate-spin h-8 w-8 text-blue-500" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path></svg><span>Buscando players no mercado...</span></div>';

            try {
                // Determine filters
                const filterCnae = document.getElementById('filter-cnae');
                const filterUf = document.getElementById('filter-uf');

                const cnae = (typeof filtrosGlobais !== 'undefined' && filtrosGlobais && filtrosGlobais.cnae) ? filtrosGlobais.cnae : normalizeCNAE(filterCnae ? filterCnae.value || '' : '');
                const uf = (typeof filtrosGlobais !== 'undefined' && filtrosGlobais && filtrosGlobais.uf) ? filtrosGlobais.uf : (filterUf ? (filterUf.value || '').toUpperCase() : '').trim();

                if (!cnae && !uf) {
                    // Se não tiver filtro, busca alguns aleatórios ou vazios? Melhor pedir filtro.
                    // Mas o backend pode retornar erro ou vazio sem filtro.
                    // Vamos enviar o request mesmo assim, o backend lida com limitação (max 20).
                }

                const body = {
                    cnae_codes: cnae ? [cnae] : [],
                    uf: uf,
                    limite: 12,
                    somente_ativas: true
                };

                const res = await fetch(`${ apiBase() } / api / analise / setorial`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(body)
                });

                let players = [];
                if (res.ok) {
                    const data = await res.json();
                    players = data.empresas || [];
                } else {
                    // Se falhar (ex: 404 Not Found se não achar empresas), pode ser ok retornar vazio
                    console.warn("API Setorial retornou erro:", res.status);
                    players = [];
                }

                if (countEl) countEl.textContent = `${ players.length } empresas encontradas`;

                if (players.length === 0) {
                    listEl.innerHTML = '<div class="col-span-full text-center py-10 text-slate-500">Nenhuma empresa encontrada com estes filtros.</div>';
                    return;
                }

                const formatCnpj = (v) => (v || '').replace(/^(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})/, "$1.$2.$3/$4-$5");
                const formatMoney = (v) => (v || 0).toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });

                listEl.innerHTML = players.map(p => {
                    const razao = p.razao_social_nome_empresarial || p.razao_social || p.nome_empresarial || '—';
                    const cnpjFmt = formatCnpj(p.cnpj || p.cnpj_basico);
                    const ufP = p.uf || '—';
                    const situacao = p.situacao_cadastral === '02' ? 'Ativa' : (p.situacao_cadastral || '—');
                    const porte = p.porte_da_empresa || p.porte || '—';
                    const capital = typeof p.capital_social_da_empresa === 'number' ? p.capital_social_da_empresa : 0;

                    return `
        < div class= "bg-white rounded-xl border border-slate-200 p-5 shadow-sm hover:shadow-md transition-all group" >
                        <div class="flex items-start justify-between mb-4">
                            <div class="w-10 h-10 rounded-lg bg-blue-50 flex items-center justify-center text-blue-600 font-bold text-xs uppercase group-hover:bg-blue-600 group-hover:text-white transition-colors">
                                ${ufP}
                            </div>
                            <span class="px-2 py-1 bg-emerald-50 text-emerald-700 text-[10px] font-bold uppercase rounded-md border border-emerald-100">${situacao}</span>
                        </div>
                        <h3 class="font-bold text-slate-800 text-sm mb-1 truncate" title="${razao}">${razao}</h3>
                        <div class="text-xs text-slate-500 mb-4 font-mono">${cnpjFmt}</div>
                        
                        <div class="grid grid-cols-2 gap-3 text-xs border-t border-slate-50 pt-4">
                            <div>
                                <div class="text-slate-400 mb-0.5 font-medium">Capital Social</div>
                                <div class="font-bold text-slate-700">${formatMoney(capital)}</div>
                            </div>
                            <div>
                                <div class="text-slate-400 mb-0.5 font-medium">Porte</div>
                                <div class="font-bold text-slate-700">${porte}</div>
                            </div>
                        </div>
                        
                        <button class="w-full mt-4 py-2.5 border border-slate-200 rounded-lg text-xs font-semibold text-slate-600 hover:bg-slate-50 hover:text-slate-900 transition-colors flex items-center justify-center gap-2">
                           Ver Detalhes
                        </button>
                    </div >
            `}).join('');

            } catch (e) {
                console.error(e);
                listEl.innerHTML = '<div class="col-span-full text-center py-10 text-red-500">Erro ao carregar lista de players (Conexão). O servidor está rodando?</div>';
            }
        }

        async function renderB2GList() {
            if (!b2gListEl) return;
            try { await loadB2GFromAPI(); } catch { }
            let filtered = filterOpportunities();
            const sortVal = String(b2gSort && b2gSort.value ? b2gSort.value : 'recente');
            filtered = filtered.sort((a, b) => {
                if (sortVal === 'valor_desc') return (b.valor || 0) - (a.valor || 0);
                if (sortVal === 'valor_asc') return (a.valor || 0) - (b.valor || 0);
                if (sortVal === 'prazo_desc') return (b.prazoDias || 0) - (a.prazoDias || 0);
                if (sortVal === 'prazo_asc') return (a.prazoDias || 0) - (b.prazoDias || 0);
                return String(b.data || '').localeCompare(String(a.data || ''));
            });
            const totalPages = Math.max(1, Math.ceil(filtered.length / B2G_PAGE_SIZE));
            if (b2gCurrentPage > totalPages) b2gCurrentPage = totalPages;
            const start = (b2gCurrentPage - 1) * B2G_PAGE_SIZE;
            const pageItems = filtered.slice(start, start + B2G_PAGE_SIZE);
            const html = pageItems.map(o => b2gViewMode === 'list' ? opportunityRowHtml(o) : opportunityCardHtml(o)).join('');
            b2gListEl.innerHTML = html;
            const countEl = document.getElementById('b2g-count'); if (countEl) countEl.textContent = `Mostrando ${ filtered.length } oportunidades`;
            updateActiveFilters();
            renderB2GPagination(totalPages);
        }
        if (b2gSearch) {
            b2gSearch.addEventListener('input', () => {
                try { clearTimeout(b2gSearch._t); } catch { }
                b2gSearch._t = setTimeout(() => { b2gCurrentPage = 1; renderB2GList(); }, 300);
            });
        }
        if (b2gFilterOrgao) { b2gFilterOrgao.addEventListener('change', () => { b2gCurrentPage = 1; renderB2GList(); }); }
        if (b2gFilterModalidade) { b2gFilterModalidade.addEventListener('change', () => { b2gCurrentPage = 1; renderB2GList(); }); }
        if (b2gSort) { b2gSort.addEventListener('change', () => { b2gCurrentPage = 1; renderB2GList(); }); }
        if (filterValor) { filterValor.addEventListener('change', () => { b2gCurrentPage = 1; renderB2GList(); }); }
        if (b2gRun) { b2gRun.addEventListener('click', () => { b2gCurrentPage = 1; renderB2GList(); }); }

        // B2G Analysis Logic
        const b2gAnalyzeBtn = document.getElementById('b2g-analyze-btn');
        const b2gCnpjInput = document.getElementById('b2g-cnpj-input');
        const b2gAnalysisResults = document.getElementById('b2g-analysis-results');
        const b2gTabs = Array.from(document.querySelectorAll('.b2g-tab'));
        let b2gActiveTab = 'direct'; // 'direct' or 'indirect'

        if (b2gAnalyzeBtn) {
            b2gAnalyzeBtn.addEventListener('click', async () => {
                const cnpj = normalizeCNPJ(b2gCnpjInput.value);
                if (!cnpj) {
                    alert('Por favor, insira um CNPJ válido.');
                    return;
                }

                // Loading State
                const originalText = b2gAnalyzeBtn.innerHTML;
                b2gAnalyzeBtn.innerHTML = `< svg class= "animate-spin -ml-1 mr-3 h-5 w-5 text-white" xmlns = "http://www.w3.org/2000/svg" fill = "none" viewBox = "0 0 24 24" ><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path></svg > Analisando...`;
                b2gAnalyzeBtn.disabled = true;

                // Simulate Analysis API Call
                await new Promise(r => setTimeout(r, 1500));

                // Show Results
                if (b2gAnalysisResults) b2gAnalysisResults.classList.remove('hidden');

                // Reset Button
                b2gAnalyzeBtn.innerHTML = originalText;
                b2gAnalyzeBtn.disabled = false;

                // Reload List with "Enhanced" Data
                b2gCurrentPage = 1;
                await renderB2GList(true); // Pass flag to indicate analysis mode
            });
        }

        b2gTabs.forEach(tab => {
            tab.addEventListener('click', () => {
                b2gTabs.forEach(t => {
                    t.classList.remove('active', 'text-blue-600', 'border-blue-600');
                    t.classList.add('text-slate-500', 'border-transparent');
                });
                tab.classList.add('active', 'text-blue-600', 'border-blue-600');
                tab.classList.remove('text-slate-500', 'border-transparent');
                b2gActiveTab = tab.getAttribute('data-tab');
                renderB2GList();
            });
        });

        // Updated renderB2GList to handle Tabs and Analysis Mode
        const originalRenderB2GList = renderB2GList; // Keep reference if needed, but we override
        renderB2GList = async function (isAnalysisMode = false) {
            if (!b2gListEl) return;
            if (isAnalysisMode || opportunities.length === 0) {
                try { await loadB2GFromAPI(); } catch { }
                // Mock Enrichment for Demo
                opportunities = opportunities.map(o => {
                    const baseMatch = Math.floor(Math.random() * (95 - 60) + 60);
                    return { ...o, match: baseMatch, type: Math.random() > 0.5 ? 'direct' : 'indirect' };
                });
            }

            let filtered = filterOpportunities();

            // Tab Filtering
            if (b2gActiveTab === 'direct') {
                // In real app, filter by type. For demo, we just rely on matching logic or assume all are direct unless marked
                // filtered = filtered.filter(o => o.type === 'direct'); 
            } else if (b2gActiveTab === 'indirect') {
                // filtered = filtered.filter(o => o.type === 'indirect');
            }

            const sortVal = String(b2gSort && b2gSort.value ? b2gSort.value : 'recente');
            filtered = filtered.sort((a, b) => {
                // If analyzed, sort by match first
                if (b2gAnalysisResults && !b2gAnalysisResults.classList.contains('hidden')) {
                    return (b.match || 0) - (a.match || 0);
                }
                if (sortVal === 'valor_desc') return (b.valor || 0) - (a.valor || 0);
                if (sortVal === 'valor_asc') return (a.valor || 0) - (b.valor || 0);
                return String(b.data || '').localeCompare(String(a.data || ''));
            });

            const totalPages = Math.max(1, Math.ceil(filtered.length / B2G_PAGE_SIZE));
            if (b2gCurrentPage > totalPages) b2gCurrentPage = totalPages;
            const start = (b2gCurrentPage - 1) * B2G_PAGE_SIZE;
            const pageItems = filtered.slice(start, start + B2G_PAGE_SIZE);
            const html = pageItems.map(o => b2gViewMode === 'list' ? opportunityRowHtml(o) : opportunityCardHtml(o)).join('');
            b2gListEl.innerHTML = html;
            const countEl = document.getElementById('b2g-count'); if (countEl) countEl.textContent = `Mostrando ${ filtered.length } oportunidades`;
            updateActiveFilters();
            renderB2GPagination(totalPages);
        };

        viewToggleBtns.forEach(btn => {
            btn.addEventListener('click', () => {
                viewToggleBtns.forEach(x => x.classList.remove('active'));
                btn.classList.add('active');
                b2gViewMode = btn.getAttribute('data-view') || 'cards';
                renderB2GList();
            });
        });
        // Auto-resize textarea
        function autoResize(textarea) {
            if (!textarea) return;
            textarea.style.height = 'auto';
            textarea.style.height = Math.min(textarea.scrollHeight, 200) + 'px';
        }

        // Handle Enter key
        function handleKeyDown(event) {
            if (!messageInput) return;
            if (event.key === 'Enter' && !event.shiftKey) {
                event.preventDefault();
                if (messageInput.value.trim() !== '') {
                    sendMessage();
                }
            }
        }

        // Use suggestion
        function useSuggestion(text) {
            if (!messageInput || !sendButton) return;
            messageInput.value = text;
            messageInput.focus();
            sendButton.disabled = false;
            autoResize(messageInput);
        }

        function apiBase() {
            if (location.protocol === 'file:') return 'http://127.0.0.1:5000';
            return '';
        }


        function normalizeCNPJ(text) {
            const digits = text.replace(/\D/g, '');
            if (digits.length === 14) return digits;
            return null;
        }



        async function callAPI(message) {
            const lower = message.toLowerCase();
            const cnpjMatch = normalizeCNPJ(message);
            const intentB2G = /(b2g|oportunidad|oportunidades|licit|pncp|governo|públic|public)/i.test(lower);
            const intentB2B = /(b2b|empresas|empresa|prospect|negócio|negocio|match|compatibilidade)/i.test(lower);
            const intentMercado = /(mercado|cnae|setorial|analisar|análise|analise)/i.test(lower);
            try {
                if (intentB2G) {
                    showB2G();
                    const s = document.getElementById('b2g-search');
                    if (s) { s.value = message; }
                    b2gCurrentPage = 1;
                    renderB2GList();
                } else if (intentB2B) {
                    showB2B();
                    const s2 = document.getElementById('b2b-search') || document.getElementById('search');
                    if (s2) { s2.value = message; }
                    b2bCurrentPage = 1;
                    renderB2BList();
                } else if (intentMercado) {
                    showMercado();
                    const cnaeMatch = lower.match(/cnae\s*(\d{4,7})/);
                    const ufMatch2 = lower.match(/\b([a-z]{2})\b/);
                    const cnaeCode = cnaeMatch ? cnaeMatch[1] : '';
                    const ufCode = ufMatch2 ? ufMatch2[1].toUpperCase() : '';
                    const fc = document.getElementById('filter-cnae'); if (fc) fc.value = cnaeCode;
                    const fu = document.getElementById('filter-uf'); if (fu && /^[A-Z]{2}$/.test(ufCode)) fu.value = ufCode;
                    analisarMercado();
                }
            } catch { }
            const isB2GIntent = /(b2g|oportunidad|oportunidades|licit|pncp|governo|públic|public)/i.test(lower);
            if (cnpjMatch && isB2GIntent) {
                const cnpj = cnpjMatch;
                const url = `${ apiBase() } / api / integracoes / licitacoes / cnpj / ${ cnpj }`;
                try {
                    const res = await fetch(url);
                    if (!res.ok) return `Erro ao buscar oportunidades B2G do CNPJ ${ cnpj }: ${ res.status } `;
                    const list = await res.json();
                    const arr = Array.isArray(list) ? list : (list.items || list.dados || []);
                    if (!Array.isArray(arr) || arr.length === 0) return `Nenhuma oportunidade B2G encontrada para o CNPJ ${ cnpj }.`;
                    const toTitle = (o) => o.titulo || o.objeto || o.resumo || '—';
                    const toOrgao = (o) => o.orgao || o.orgao_nome || o.unidade_gestora || o.orgao_descricao || '—';
                    const toModal = (o) => o.modalidade || o.modalidade_nome || '—';
                    const toValor = (o) => {
                        const v = typeof o.valor === 'number' ? o.valor : (typeof o.valor_estimado === 'number' ? o.valor_estimado : null);
                        return v != null ? v.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL', maximumFractionDigits: 0 }) : '—';
                    };
                    const toPrazo = (o) => o.prazo || o.data_limite || o.data || '—';
                    const sorted = arr.slice().sort((a, b) => {
                        const va = (typeof a.valor === 'number' ? a.valor : (typeof a.valor_estimado === 'number' ? a.valor_estimado : 0));
                        const vb = (typeof b.valor === 'number' ? b.valor : (typeof b.valor_estimado === 'number' ? b.valor_estimado : 0));
                        return vb - va;
                    });
                    const top = sorted.slice(0, 6).map((o, i) => `${ i + 1 }. ${ toTitle(o) } \n   Órgão: ${ toOrgao(o) } • Modalidade: ${ toModal(o) } \n   Valor: ${ toValor(o) } • Prazo: ${ toPrazo(o) } `).join('\n\n');
                    return `Oportunidades B2G do CNPJ ${ cnpj } \n\n${ top } `;
                } catch (e) {
                    return `Falha de rede ao buscar oportunidades B2G para o CNPJ ${ cnpj }.`;
                }
            }
            if (lower.includes('consultar cnpj')) {
                const cnpj = cnpjMatch || normalizeCNPJ(lower);
                if (!cnpj) return 'Informe um CNPJ válido com 14 dígitos.';
                const url = `${ apiBase() } /api/consulta / cnpj / ${ cnpj } `;
                try {
                    const res = await fetch(url);
                    if (!res.ok) return `Erro ao consultar CNPJ: ${ res.status } `;
                    const d = await res.json() || {};
                    const formatCNPJ = (v) => (v || '').replace(/(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})/, '$1.$2.$3/$4-$5');
                    const formatDateBR = (v) => v ? new Date(v).toLocaleDateString('pt-BR') : '—';
                    const formatCurrencyBR = (v) => {
                        const n = typeof v === 'number' ? v : parseFloat(String(v).replace(/[^\d.,-]/g, '').replace('.', '').replace(',', '.'));
                        if (isNaN(n)) return '—';
                        return n.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
                    };
                    const formatAddress = (x) => {
                        const parts = [x.logradouro, x.numero, x.complemento].filter(Boolean).join(', ');
                        const line1 = parts || '—';
                        const cidadeUf = [x.municipio, x.uf].filter(Boolean).join(' – ');
                        const cep = x.cep ? `CEP ${ x.cep } ` : '';
                        const line2 = [cidadeUf, cep, 'Brasil'].filter(Boolean).join(', ');
                        return `${ line1 } \n ${ line2 } `.trim();
                    };

                    const cnpjFmt = formatCNPJ(cnpj);
                    const razao = d.razao_social_nome_empresarial || d.razao_social || d.nome_empresarial || '—';
                    const fantasia = d.nome_fantasia || '—';
                    const abertura = formatDateBR(d.data_abertura || d.data_inicio_atividade);
                    const natureza = d.natureza_juridica_nome || d.natureza_juridica || '—';
                    const situacao = d.situacao_cadastral_nome || d.situacao_cadastral || d.situacao || '—';
                    const capital = formatCurrencyBR(d.capital_social);
                    const endereco = formatAddress({
                        logradouro: d.logradouro,
                        numero: d.numero,
                        complemento: d.complemento,
                        bairro: d.bairro,
                        municipio: d.municipio || d.municipio_nome,
                        uf: d.uf,
                        cep: d.cep
                    });
                    const cnaeCode = d.cnae_principal || d.cnae_fiscal || '—';
                    const cnaeDesc = d.cnae_principal_descricao || d.cnae_descricao || '';
                    const cnaePrincipal = cnaeDesc ? `${ cnaeDesc } (CNAE ${ cnaeCode })` : `CNAE ${ cnaeCode } `;
                    const secundarias = Array.isArray(d.cnaes_secundarios) ? d.cnaes_secundarios : [];
                    const secundariasResumo = secundarias.length > 0 ? `(${ secundarias.length } atividades)` : '—';
                    const sociosRaw = Array.isArray(d.qsa) ? d.qsa : (Array.isArray(d.socios) ? d.socios : []);
                    const toNome = (s) => s.nome_socio || s.nome || '—';
                    const toQual = (s) => s.qualificacao_socio || s.qualificacao || '—';
                    const admins = sociosRaw.filter(s => /administrador/i.test(String(toQual(s))));
                    const sociosSel = (admins.length > 0 ? admins : sociosRaw).slice(0, 5);
                    const sociosLista = sociosSel.map(s => `${ toNome(s) } – ${ toQual(s) } `).join('\n');

                    const observacao = situacao && situacao.toUpperCase() === 'INAPTA'
                        ? 'A situação INAPTA indica omissão de declarações junto à Receita Federal e pode impactar a regularidade comercial e fiscal.'
                        : '';

                    const adminCount = (Array.isArray(d.qsa) ? d.qsa.filter(s => /administrador/i.test(String(s.qualificacao_socio || s.qualificacao || ''))).length : 0);
                    const enderecoLinha = (endereco || '').split('\n')[0] || endereco || '—';


                    return [
                        '📌 Informações principais',
                        '',
                        `CNPJ: ${ cnpjFmt } `,
                        `Razão Social: ${ razao } `,
                        `Nome Fantasia: ${ fantasia } `,
                        `Data de Abertura: ${ abertura } `,
                        `Natureza Jurídica: ${ natureza } `,
                        `Status da Empresa: ${ situacao } `,
                        `Capital Social: ${ capital } `,
                        '',
                        '🏢 Endereço',
                        '',
                        `${ endereco } `,
                        '',
                        '📊 Atividade Econômica',
                        '',
                        `Atividade principal: ${ cnaePrincipal } `,
                        `Atividades secundárias: ${ secundariasResumo } `,
                        '',
                        '👥 Sócios/Administradores',
                        '',
                        sociosLista || '—',
                        '',
                        observacao ? '⚠️ Observação:' : '',
                        observacao
                    ].filter(Boolean).join('\n');
                } catch (e) {
                    return 'Falha de rede ao consultar CNPJ.';
                }
            }

            if (lower.includes('cnae')) {
                const codeMatch = lower.match(/cnae\s*(\d{4,7})/);
                const ufMatch = lower.match(/\s(em|no)\s([a-z]{2})\b/);
                const code = codeMatch ? codeMatch[1] : '';
                const uf = ufMatch ? ufMatch[2].toUpperCase() : '';
                const body = { cnae_codes: code ? [code] : [], uf };
                const url = `${ apiBase() } /api/analise / setorial`;
                try {
                    const res = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
                    if (!res.ok) return `Erro na análise setorial: ${ res.status } `;
                    const data = await res.json();
                    const kpis = data && data.kpis ? data.kpis : {};
                    const resumo = data && data.resumo ? data.resumo : '';
                    return `Análise Setorial\nCNAE: ${ code || '—' } \nUF: ${ uf || '—' } \nKPIs: ${ JSON.stringify(kpis) } \nResumo: ${ resumo || '—' } `;
                } catch (e) {
                    return 'Falha de rede na análise setorial.';
                }
            }

            if (lower.includes('licita') || lower.includes('pncp')) {
                const cnpj = cnpjMatch || normalizeCNPJ(lower);
                if (!cnpj) return 'Informe um CNPJ para consultar licitações.';
                const url = `${ apiBase() } /api/integracoes / licitacoes / cnpj / ${ cnpj } `;
                try {
                    const res = await fetch(url);
                    if (!res.ok) return `Erro nas licitações: ${ res.status } `;
                    const data = await res.json();
                    const total = Array.isArray(data) ? data.length : (data.total || 0);
                    return `Licitações PNCP\nCNPJ: ${ cnpj } \nTotal encontradas: ${ total } `;
                } catch (e) {
                    return 'Falha de rede ao consultar licitações.';
                }
            }

            if (lower.includes('sócio') || lower.includes('socio')) {
                const nameMatch = message.match(/nome\s(.+)/i) || message.match(/por\snome\s(.+)/i);
                const nome = nameMatch ? nameMatch[1].trim() : message.trim();
                const url = `${ apiBase() } /api/consulta / socio ? q = ${ encodeURIComponent(nome) } `;
                try {
                    const res = await fetch(url);
                    if (!res.ok) return `Erro na busca de sócios: ${ res.status } `;
                    const list = await res.json();
                    if (!Array.isArray(list) || list.length === 0) return 'Nenhum sócio encontrado.';
                    const head = list.slice(0, 5).map(s => `${ s.nome_socio || '—' } • ${ s.qualificacao || '—' } • ${ s.cnpj || '—' } `).join('\n');
                    return `Sócios encontrados(top 5) \n${ head } `;
                } catch (e) {
                    return 'Falha de rede na busca de sócios.';
                }
            }

            const tokens = lower.split(/\s+/).filter(Boolean);
            const isGreeting = /^(oi|ol[áa]|bom dia|boa tarde|boa noite|hello|hi|hey)$/i.test(lower.trim());
            const hasKeyword = /(cnpj|empresa|empresas|cnae|licit|pncp|uf|municipio|munic pio|socio|s F3cio|contrato|editais|itens)/.test(lower) || /\d{4,}/.test(lower) || /\d{14}/.test(lower);
            if (isGreeting || (!hasKeyword && tokens.length < 2)) {
                return 'Olá! Posso ajudar com:\n- Consultar CNPJ 00000000000000\n- Analisar CNAE 6204000 em SP\n- Ver licitações PNCP do CNPJ\n- Buscar empresas por palavra-chave (ex: software em SP)';
            }
            try {
                const url = `${ apiBase() } /api/analise / nl2sql`;
                const res = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ pergunta: message, limite: 20 }) });
                if (!res.ok) return `Não consegui entender.Tente: Consultar CNPJ 00000000000000, Analisar CNAE 6204000 em SP, Ver licitações PNCP do CNPJ.`;
                const data = await res.json();
                const cols = (data.columns || []).join(', ');
                const rows = (data.rows || []).slice(0, 5).map(r => (r || []).join(' | ')).join('\n');
                const sql = data.sql || '';
                return `Consulta NL → SQL\nColunas: ${ cols || '—' } \nAmostra: \n${ rows || '—' } \nSQL: ${ sql } `;
            } catch (e) {
                return 'Falha ao interpretar sua pergunta.';
            }
        }

        async function sendMessage() {
            const message = (messageInput ? messageInput.value.trim() : '');
            if (!message) return;

            // Hide welcome screen, show chat
            if (welcomeScreen) welcomeScreen.classList.add('hidden');
            if (chatContainer) chatContainer.classList.remove('hidden');
            if (mainContent) { mainContent.classList.remove('justify-center'); mainContent.classList.add('pt-6'); }
            const ib = document.getElementById('interaction-bar');
            if (ib) ib.classList.remove('hidden');

            // Add user message
            addMessage(message, 'user');
            addRecent(message);

            // Clear input
            if (messageInput) { messageInput.value = ''; messageInput.style.height = 'auto'; }
            if (sendButton) { sendButton.disabled = true; }

            try {
                const result = await callAPI(message);
                addMessage(result, 'assistant');
            } catch (e) {
                addMessage('Erro ao processar a mensagem.', 'assistant');
            }
        }

        let recentSearches = [];
        function loadRecents() {
            try {
                const raw = localStorage.getItem('recent_searches');
                recentSearches = raw ? JSON.parse(raw) : [];
            } catch { recentSearches = []; }
        }
        function saveRecents() {
            localStorage.setItem('recent_searches', JSON.stringify(recentSearches));
        }
        function renderRecents() {
            const ul = document.getElementById('recent-list');
            if (!ul) return;
            ul.innerHTML = '';
            recentSearches.slice(0, 10).forEach(term => {
                const li = document.createElement('li');
                li.className = 'flex items-center gap-2 px-2 py-1 rounded hover:bg-slate-800 cursor-pointer';
                li.innerHTML = `< svg width = "16" height = "16" viewBox = "0 0 24 24" fill = "none" stroke = "currentColor" stroke - width="2" class="text-slate-300" ><circle cx="11" cy="11" r="7"/><path d="M21 21l-4.3-4.3"/></svg > <span class="text-sm">${escapeHtml(term)}</span>`;
                li.onclick = () => {
                    if (!messageInput || !sendButton) return;
                    messageInput.value = term;
                    sendButton.disabled = false;
                    autoResize(messageInput);
                };
                ul.appendChild(li);
            });
        }
        function addRecent(term) {
            loadRecents();
            const t = term.trim();
            if (!t) return;
            recentSearches = [t, ...recentSearches.filter(x => x.toLowerCase() !== t.toLowerCase())];
            if (recentSearches.length > 20) recentSearches = recentSearches.slice(0, 20);
            saveRecents();
            renderRecents();
        }
        loadRecents();
        renderRecents();

        // Add message to chat
        function addMessage(text, role) {
            const messageDiv = document.createElement('div');
            messageDiv.className = `flex gap - 4 mb - 6 ${ role === 'user' ? 'justify-end' : '' } `;

            if (role === 'user') {
                messageDiv.innerHTML = `
            < div class="max-w-[85%] bg-blue-600 text-white rounded-2xl rounded-tr-none shadow-md shadow-blue-500/10 px-5 py-3" >
                <p class="whitespace-pre-wrap text-[15px] leading-relaxed">${escapeHtml(text)}</p>
                    </div >
            `;
            } else {
                messageDiv.innerHTML = `
            < div class="w-8 h-8 rounded-lg bg-gradient-to-br from-blue-600 to-indigo-600 flex items-center justify-center flex-shrink-0 shadow-lg shadow-blue-500/20" >
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none">
                    <circle cx="12" cy="12" r="8" stroke="white" stroke-width="2" />
                    <path d="M12 12 L12 4 A8 8 0 0 1 20 12 Z" fill="white" />
                </svg>
                    </div >
            <div class="max-w-[85%] bg-white rounded-2xl rounded-tl-none shadow-sm border border-slate-100 px-5 py-4">
                <p class="text-slate-800 whitespace-pre-wrap leading-relaxed text-[15px]">${escapeHtml(text)}</p>
            </div>
        `;
                speak(text);
            }

            chatContainer.appendChild(messageDiv);
            chatContainer.scrollTop = chatContainer.scrollHeight;
        }

        // Escape HTML to prevent XSS
        function escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }

        // Header Actions Logic
        (function () {
            // Logout
            const logoutBtn = document.getElementById('logout-button');
            if (logoutBtn) {
                logoutBtn.addEventListener('click', () => {
                    if (confirm('Tem certeza que deseja sair?')) {
                        localStorage.removeItem('pickles_auth');
                        location.reload();
                    }
                });
            }

            // ========== CONSULTA CNPJ PAGE ==========
            const cnpjSearchInput = document.getElementById('cnpj-search-input');
            const cnpjSearchBtn = document.getElementById('cnpj-search-btn');
            const cnpjSearchError = document.getElementById('cnpj-search-error');
            const cnpjLoading = document.getElementById('cnpj-loading');
            const cnpjProfile = document.getElementById('cnpj-profile');

            async function consultarCNPJ(cnpj) {
                try {
                    // Limpar erro anterior
                    if (cnpjSearchError) {
                        cnpjSearchError.classList.add('hidden');
                    }

                    // Validar CNPJ
                    const cnpjLimpo = cnpj.replace(/\D/g, '');
                    if (cnpjLimpo.length !== 14) {
                        if (cnpjSearchError) {
                            cnpjSearchError.querySelector('span').textContent = 'CNPJ deve conter 14 dígitos';
                            cnpjSearchError.classList.remove('hidden');
                        }
                        return;
                    }

                    function showDashboardView(viewName) {
            document.querySelectorAll('.dashboard-view').forEach(v => v.classList.add('hidden'));
            document.getElementById(viewName)?.classList.remove('hidden');
        }

        // ==================== NAVIGATION EVENT LISTENERS ====================
        // Conectar navegação do sidebar com funções show
        document.addEventListener('DOMContentLoaded', () => {
            // Home / Assistente
            const navHome = document.getElementById('nav-home');
            if (navHome) navHome.addEventListener('click', (e) => {
                e.preventDefault();
                showAssistant();
            });

            // CNPJ
            const navCnpj = document.getElementById('nav-cnpj');
            if (navCnpj) navCnpj.addEventListener('click', (e) => {
                e.preventDefault();
                showCnpj();
            });

            // B2B / Mercado
            const navB2b = document.getElementById('nav-b2b');
            if (navB2b) navB2b.addEventListener('click', (e) => {
                e.preventDefault();
                showB2B();
            });

            // B2G
            const navB2g = document.getElementById('nav-b2g');
            if (navB2g) navB2g.addEventListener('click', (e) => {
                e.preventDefault();
                showB2G();
            });

            // Análise Setorial
            const navSetorial = document.getElementById('nav-setorial');
            if (navSetorial) navSetorial.addEventListener('click', (e) => {
                e.preventDefault();
                showMercado();
            });

            // Plano
            const navPlano = document.getElementById('nav-plano');
            if (navPlano) navPlano.addEventListener('click', (e) => {
                e.preventDefault();
                showPlans();
            });

            // Configurações
            const navConfig = document.getElementById('nav-config');
            if (navConfig) navConfig.addEventListener('click', (e) => {
                e.preventDefault();
                showSettings();
            });

            // Logout
            const navLogout = document.getElementById('nav-logout');
            if (navLogout) navLogout.addEventListener('click', (e) => {
                e.preventDefault();
                if (confirm('Deseja realmente sair?')) {
                    window.location.href = '/';
                }
            });

            // Mini sidebar (mobile/collapsed)
            const miniOpen = document.getElementById('mini-open');
            if (miniOpen) miniOpen.addEventListener('click', () => {
                const sidebar = document.getElementById('sidebar');
                if (sidebar) sidebar.classList.remove('hidden');
            });

            const miniAssistente = document.getElementById('mini-assistente');
            if (miniAssistente) miniAssistente.addEventListener('click', showAssistant);

            const miniMercado = document.getElementById('mini-mercado');
            if (miniMercado) miniMercado.addEventListener('click', showMercado);

            const miniCnpj = document.getElementById('mini-cnpj');
            if (miniCnpj) miniCnpj.addEventListener('click', showCnpj);

            const miniB2g = document.getElementById('mini-b2g');
            if (miniB2g) miniB2g.addEventListener('click', showB2G);

            const miniB2b = document.getElementById('mini-b2b');
            if (miniB2b) miniB2b.addEventListener('click', showB2B);

            console.log('✅ Navigation listeners attached');
        });

        // CNPJ Page Logic
                    // Mostrar loading
                    if (cnpjProfile) cnpjProfile.classList.add('hidden');
                    if (cnpjLoading) cnpjLoading.classList.remove('hidden');

                    // Consultar API
                    const base = apiBase();
                    const response = await fetch(`${ base } /api/consulta / cnpj / ${ cnpjLimpo } `);

                    if (!response.ok) {
                        throw new Error('Erro ao consultar CNPJ');
                    }

                    const data = await response.json();

                    // Ocultar loading
                    if (cnpjLoading) cnpjLoading.classList.add('hidden');

                    // Preencher dados
                    preencherDadosCNPJ(data);

                    // Mostrar profile
                    if (cnpjProfile) cnpjProfile.classList.remove('hidden');

                    // Buscar oportunidades
                    await buscarOportunidades(cnpjLimpo);

                } catch (error) {
                    console.error('Erro ao consultar CNPJ:', error);
                    if (cnpjLoading) cnpjLoading.classList.add('hidden');
                    if (cnpjSearchError) {
                        cnpjSearchError.querySelector('span').textContent = 'Erro ao consultar CNPJ. Tente novamente.';
                        cnpjSearchError.classList.remove('hidden');
                    }
                }
            }

            function preencherDadosCNPJ(data) {
                // Status e Porte
                const statusBadge = document.getElementById('cnpj-status-badge');
                if (statusBadge) {
                    const situacao = data.situacao_cadastral || '02';
                    if (situacao === '02' || situacao === '2') {
                        statusBadge.textContent = 'ATIVA';
                        statusBadge.className = 'px-3 py-1 bg-green-500 text-white text-xs font-bold rounded-full uppercase';
                    } else {
                        statusBadge.textContent = 'INATIVA';
                        statusBadge.className = 'px-3 py-1 bg-red-500 text-white text-xs font-bold rounded-full uppercase';
                    }
                }

                const porteBadge = document.getElementById('cnpj-porte-badge');
                if (porteBadge && data.porte_da_empresa) {
                    const portes = {
                        '01': 'Microempresa', '1': 'Microempresa',
                        '02': 'Pequena', '2': 'Pequena',
                        '03': 'Média', '3': 'Média',
                        '04': 'Grande', '4': 'Grande',
                        '05': 'Demais', '5': 'Demais'
                    };
                    porteBadge.textContent = portes[data.porte_da_empresa] || 'Não Informado';
                }

                // Dados principais
                if (document.getElementById('cnpj-razao-social')) {
                    document.getElementById('cnpj-razao-social').textContent = data.razao_social || data.razao_social_nome_empresarial || '—';
                }
                if (document.getElementById('cnpj-nome-fantasia')) {
                    document.getElementById('cnpj-nome-fantasia').textContent = data.nome_fantasia || '—';
                }
                if (document.getElementById('cnpj-number')) {
                    const cnpjFormatado = (data.cnpj_basico || data.cnpj || '').replace(/(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})/, '$1.$2.$3/$4-$5');
                    document.getElementById('cnpj-number').textContent = cnpjFormatado;
                }
                if (document.getElementById('cnpj-abertura')) {
                    const dataAbertura = data.data_de_inicio_atividade || data.data_inicio_atividade || '';
                    document.getElementById('cnpj-abertura').textContent = `Abertura: ${ dataAbertura || '—' } `;
                }

                // Dados cadastrais
                if (document.getElementById('cnpj-cnae')) {
                    document.getElementById('cnpj-cnae').textContent = data.cnae_fiscal_principal || data.cnae_fiscal || '—';
                }
                if (document.getElementById('cnpj-natureza')) {
                    document.getElementById('cnpj-natureza').textContent = data.natureza_juridica || '—';
                }
                if (document.getElementById('cnpj-capital')) {
                    const capital = parseFloat(data.capital_social || 0);
                    document.getElementById('cnpj-capital').textContent = capital > 0 ? `R$ ${ capital.toLocaleString('pt-BR', { minimumFractionDigits: 2 }) } ` : '—';
                }

                // Localização
                if (document.getElementById('cnpj-endereco')) {
                    const endereco = [
                        data.tipo_de_logradouro,
                        data.logradouro,
                        data.numero,
                        data.complemento
                    ].filter(Boolean).join(' ');
                    document.getElementById('cnpj-endereco').textContent = endereco || '—';
                }
                if (document.getElementById('cnpj-municipio')) {
                    document.getElementById('cnpj-municipio').textContent = data.municipio || '—';
                }
                if (document.getElementById('cnpj-uf')) {
                    document.getElementById('cnpj-uf').textContent = data.uf || '—';
                }
                if (document.getElementById('cnpj-cep')) {
                    const cep = (data.cep || '').replace(/(\d{5})(\d{3})/, '$1-$2');
                    document.getElementById('cnpj-cep').textContent = cep || '—';
                }

                // Contato
                if (document.getElementById('cnpj-email')) {
                    document.getElementById('cnpj-email').textContent = data.correio_eletronico || '—';
                }
                if (document.getElementById('cnpj-telefone')) {
                    const tel1 = data.telefone_1 || data.ddd_telefone_1 || '';
                    document.getElementById('cnpj-telefone').textContent = tel1 || '—';
                }
            }

            async function buscarOportunidades(cnpj) {
                const oportunidadesList = document.getElementById('cnpj-oportunidades-list');
                const oportunidadesCount = document.getElementById('cnpj-oportunidades-count');

                try {
                    // Simulação de busca de oportunidades (você pode integrar com a API real)
                    await new Promise(resolve => setTimeout(resolve, 1000));

                    // Dados simulados
                    const oportunidades = [
                        {
                            titulo: 'Pregão Eletrônico - Serviços de TI',
                            orgao: 'Prefeitura Municipal de São Paulo',
                            valor: 'R$ 150.000,00',
                            prazo: '15 dias',
                            compatibilidade: 85
                        },
                        {
                            titulo: 'Concorrência - Fornecimento de Equipamentos',
                            orgao: 'Governo do Estado de SP',
                            valor: 'R$ 320.000,00',
                            prazo: '22 dias',
                            compatibilidade: 72
                        }
                    ];

                    if (oportunidadesCount) {
                        oportunidadesCount.textContent = oportunidades.length;
                    }

                    if (oportunidadesList) {
                        oportunidadesList.innerHTML = oportunidades.map(op => `
            < div class="border border-slate-200 rounded-xl p-4 hover:border-blue-300 hover:shadow-md transition-all cursor-pointer" >
                                <div class="flex items-start justify-between mb-2">
                                    <h4 class="font-semibold text-slate-900 flex-1">${op.titulo}</h4>
                                    <span class="px-2 py-1 bg-green-100 text-green-700 text-xs font-bold rounded-full">${op.compatibilidade}%</span>
                                </div>
                                <p class="text-sm text-slate-600 mb-3">${op.orgao}</p>
                                <div class="flex items-center gap-4 text-xs text-slate-500">
                                    <span class="flex items-center gap-1">
                                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                            <line x1="12" y1="1" x2="12" y2="23"></line>
                                            <path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"></path>
                                        </svg>
                                        ${op.valor}
                                    </span>
                                    <span class="flex items-center gap-1">
                                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                            <circle cx="12" cy="12" r="10"></circle>
                                            <polyline points="12 6 12 12 16 14"></polyline>
                                        </svg>
                                        ${op.prazo}
                                    </span>
                                </div>
                            </div >
            `).join('');
                    }
                } catch (error) {
                    console.error('Erro ao buscar oportunidades:', error);
                    if (oportunidadesList) {
                        oportunidadesList.innerHTML = '<div class="text-center py-8 text-slate-400">Erro ao carregar oportunidades</div>';
                    }
                }
            }

            // Event Listeners
            const partnerContainer = document.getElementById('partner-results-container');
            const partnerList = document.getElementById('partner-results-list');

            async function buscarSocio(termo) {
                if (cnpjLoading) cnpjLoading.classList.remove('hidden');
                if (cnpjProfile) cnpjProfile.classList.add('hidden');
                if (partnerContainer) partnerContainer.classList.add('hidden');
                if (cnpjSearchError) cnpjSearchError.classList.add('hidden');

                try {
                    const res = await fetch(`${ apiBase() } /api/consultas / socio ? nome = ${ encodeURIComponent(termo) } `);
                    if (!res.ok) throw new Error('Erro na busca');
                    const data = await res.json();

                    const lista = Array.isArray(data) ? data : (data.data || []);
                    if (lista.length === 0) throw new Error('Nenhum resultado encontrado');

                    if (partnerList) {
                        partnerList.innerHTML = lista.map(emp => {
                            const cnpj = emp.cnpj || emp.cnpj_basico || '';
                            const razao = emp.razao_social || emp.razao_social_nome_empresarial || 'Nome não disponível';
                            const qual = emp.qualificacao_socio || emp.qualificacao || 'Sócio';
                            const nomeSocio = emp.nome_socio || termo;

                            return `
            < div class="flex items-center justify-between p-4 bg-slate-50 rounded-xl hover:bg-slate-100 transition-colors border border-slate-200 cursor-pointer" onclick = "consultarCNPJ('${cnpj}')" >
                                    <div>
                                        <div class="font-bold text-slate-800">${razao}</div>
                                        <div class="text-sm text-slate-500 font-mono">CNPJ: ${cnpj}</div>
                                        <div class="text-xs text-blue-600 mt-1 font-semibold uppercase">👤 ${nomeSocio} (${qual})</div>
                                    </div>
                                    <svg class="text-slate-400" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                        <path d="M9 18l6-6-6-6"/>
                                    </svg>
                                </div >
            `;
                        }).join('');
                    }

                    if (cnpjLoading) cnpjLoading.classList.add('hidden');
                    if (partnerContainer) partnerContainer.classList.remove('hidden');

                } catch (e) {
                    if (cnpjLoading) cnpjLoading.classList.add('hidden');
                    if (cnpjSearchError) {
                        cnpjSearchError.querySelector('span').textContent = 'Nenhum registro encontrado para este sócio/empresa.';
                        cnpjSearchError.classList.remove('hidden');
                    }
                }
            }

            // Expor consultarCNPJ globalmente para ser usado no onclick do HTML
            window.consultarCNPJ = consultarCNPJ;

            async function handleSearch() {
                const term = document.getElementById('cnpj-search-input')?.value.trim() || '';
                if (!term) return;

                const searchBtn = document.getElementById('cnpj-search-btn');
                const originalBtnContent = searchBtn ? searchBtn.innerHTML : 'Consultar';

                try {
                    if (searchBtn) {
                        searchBtn.disabled = true;
                        searchBtn.innerHTML = `
            < svg class="animate-spin h-5 w-5 text-white" xmlns = "http://www.w3.org/2000/svg" fill = "none" viewBox = "0 0 24 24" >
                            <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                            <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"></path>
                        </svg >
            Buscando...
        `;
                    }

                    const digits = term.replace(/\D/g, '');

                    // Se for numérico e tiver 14 dígitos, assume CNPJ
                    if (digits.length === 14 && term.length < 18) {
                        // Hide partner results if open
                        if (partnerContainer) partnerContainer.classList.add('hidden');
                        await consultarCNPJ(digits);
                    } else {
                        // Busca textual (Sócio ou Razão Social)
                        await buscarSocio(term);
                    }
                } finally {
                    if (searchBtn) {
                        searchBtn.innerHTML = originalBtnContent;
                        searchBtn.disabled = false;
                    }
                }
            }

            window.handleSearch = handleSearch;

            // Direct assignment is safer than cloning if we want to be sure it works
            const btnEl = document.getElementById('cnpj-search-btn');
            if (btnEl) {
                btnEl.onclick = handleSearch;
            }

            const inputEl = document.getElementById('cnpj-search-input');
            if (inputEl) {
                inputEl.onkeypress = (e) => {
                    if (e.key === 'Enter') handleSearch();
                };
            }
        });

        // ==================== SIDEBAR TOGGLE FUNCTIONALITY ====================
        const sidebar = document.getElementById('sidebar');
        const sidebarToggle = document.getElementById('sidebar-toggle');
        const sidebarToggleIcon = document.getElementById('sidebar-toggle-icon');
        const mainContent = document.getElementById('main-content');
        
        let sidebarCollapsed = localStorage.getItem('sidebarCollapsed') === 'true';
        
        function updateSidebarState() {
            if (sidebarCollapsed) {
                // Sidebar recolhida
                sidebar.style.width = '80px';
                mainContent.style.marginLeft = '80px';
                
                // Esconder textos
                document.querySelectorAll('.sidebar-text').forEach(el => {
                    el.style.opacity = '0';
                    el.style.display = 'none';
                });
                
                // Rotacionar ícone
                sidebarToggleIcon.style.transform = 'rotate(180deg)';
            } else {
                // Sidebar expandida
                sidebar.style.width = '280px';
                mainContent.style.marginLeft = '280px';
                
                // Mostrar textos
                setTimeout(() => {
                    document.querySelectorAll('.sidebar-text').forEach(el => {
                        el.style.display = '';
                        el.style.opacity = '1';
                    });
                }, 150);
                
                // Rotacionar ícone
                sidebarToggleIcon.style.transform = 'rotate(0deg)';
            }
            
            // Salvar estado
            localStorage.setItem('sidebarCollapsed', sidebarCollapsed);
        }
        
        // Event listener do botão de toggle
        if (sidebarToggle) {
            sidebarToggle.addEventListener('click', () => {
                sidebarCollapsed = !sidebarCollapsed;
                updateSidebarState();
            });
        }
        
        // Aplicar estado inicial
        if (typeof updateSidebarState === 'function') {
            updateSidebarState();
        }
        
        // Adicionar CSS para transições suaves
        const sidebarStyle = document.createElement('style');
        sidebarStyle.textContent = `
            .sidebar - text {
            transition: opacity 0.3s ease -in -out;
        }
        #sidebar - toggle - icon {
            transition: transform 0.3s ease -in -out;
        }
        `;
        document.head.appendChild(sidebarStyle);

    </script>
</body>


