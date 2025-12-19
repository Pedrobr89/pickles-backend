// assets/utils.js

const API_BASE = (location && (location.protocol === 'http:' || location.protocol === 'https:')) ? '' : 'http://127.0.0.1:5000';

/**
 * Função genérica para chamar a API (Adapte esta função para a sua infraestrutura)
 */
async function callApi(path, opts) {
    try {
        let signal = opts && opts.signal;
        const to = opts && opts.timeout;
        let ctrl = null;
        if (!signal && to && typeof AbortController !== 'undefined'){
            ctrl = new AbortController();
            signal = ctrl.signal;
            setTimeout(function(){ try{ ctrl.abort() }catch{} }, Number(to));
        }
        const r = await fetch(`${API_BASE}${path}`, {
            method: (opts && opts.method) || 'GET',
            headers: Object.assign({ 'Accept': 'application/json' }, (opts && opts.headers) || {}),
            body: opts && opts.body,
            signal
        });
        const ct = r.headers.get('content-type') || '';
        if (ct.includes('application/json')) {
            return { ok: r.ok, data: await r.json() }
        }
        return { ok: r.ok, data: await r.text() }
    } catch (e) {
        const msg = String(e && e.message || e || '').toLowerCase();
        const aborted = (e && e.name === 'AbortError') || msg.includes('abort') || msg.includes('aborted');
        if (aborted) return { ok: false, aborted: true, data: null };
        return { ok: false, data: { erro: String(e && e.message || e) } }
    }
}

/**
 * Define o innerHTML de um elemento
 */
function setHtml(id, html) {
    const el = document.getElementById(id);
    if (el) el.innerHTML = html;
}

/**
 * Exibe um spinner de carregamento padronizado
 */
function showLoading(id, color = 'slate-400') {
    setHtml(id, `<div class="text-${color} loading text-center p-4">Carregando...</div>`);
}

function showInfo(id, text) {
    setHtml(id, `<div class="text-indigo-300 p-3 border border-indigo-700/40 rounded-lg bg-indigo-900/10">${text}</div>`);
}

/**
 * Exibe uma mensagem de erro padronizada
 */
function showError(id, data) {
    const d = typeof data === 'string' ? { erro: data } : data;
    setHtml(id, `<div class="text-red-400 p-4 border border-red-700/50 rounded-lg bg-red-900/10 slide-up">Erro: ${d.erro || JSON.stringify(d, null, 2)}</div>`);
}

/**
 * Debounce utilitário
 */
function debounce(func, wait){
    let timeout;
    return function(){
        const context = this, args = arguments;
        clearTimeout(timeout);
        timeout = setTimeout(function(){ func.apply(context, args) }, wait);
    }
}

function throttle(func, wait){
    let last = 0;
    let timer = null;
    return function(){
        const now = Date.now();
        const remaining = wait - (now - last);
        const context = this, args = arguments;
        if (remaining <= 0){
            last = now;
            func.apply(context, args);
        } else if (!timer){
            timer = setTimeout(function(){
                timer = null;
                last = Date.now();
                func.apply(context, args);
            }, remaining);
        }
    }
}

/**
 * Escapa HTML para prevenir XSS ao renderizar texto do usuário
 */
function escapeHtml(s){
    try{
        const str = String(s ?? '');
        return str
            .replace(/&/g,'&amp;')
            .replace(/</g,'&lt;')
            .replace(/>/g,'&gt;')
            .replace(/"/g,'&quot;')
            .replace(/\'/g,'&#39;');
    }catch{ return '' }
}

function startButtonTimer(btn, baseText, timeoutMs){
    try{
        let start = Date.now();
        btn.disabled = true;
        btn.innerHTML = baseText;
        const step = function(){
            const s = Math.floor((Date.now() - start) / 1000);
            btn.innerHTML = `${baseText} (${s}s)`;
        };
        step();
        const int = setInterval(step, 1000);
        return function(doneText){ try{ clearInterval(int) }catch{}; btn.disabled = false; if (doneText) btn.innerHTML = doneText };
    }catch{ return function(){} }
}

/**
 * Renderiza chips removíveis para listas (CNAEs, UFs, etc.)
 */
function renderChips(items, type){
    const safeItems = Array.isArray(items) ? items : [];
    return safeItems.map(function(c){
        const txt = escapeHtml(c);
        return `
            <span class="inline-flex items-center bg-slate-800 border border-slate-700 text-slate-200 px-2 py-1 rounded-full text-xs">
              ${txt}
              <button data-chip="${txt}" data-type="${escapeHtml(type)}" class="ml-1 text-slate-300 hover:text-white remove-chip">×</button>
            </span>
        `;
    }).join('');
}

document.addEventListener('click', function(e){
    if(e.target && e.target.classList && e.target.classList.contains('remove-chip')){
        const val = e.target.getAttribute('data-chip');
        const type = e.target.getAttribute('data-type');
        try{
            if(!window.state) window.state = {};
            if(type === 'cnae'){
                window.state.selectedCNAEs = (window.state.selectedCNAEs||[]).filter(function(x){ return String(x) !== String(val) });
                const input = document.getElementById('input-cnaes');
                if(input){
                    const list = input.value.split(',').map(function(s){ return s.trim() }).filter(Boolean).filter(function(x){ return String(x) !== String(val) });
                    input.value = list.join(', ');
                }
                const cont = document.getElementById('cnae-chips'); if(cont){ cont.innerHTML = renderChips(window.state.selectedCNAEs, 'cnae') }
            } else if(type === 'uf'){
                window.state.selectedUFs = (window.state.selectedUFs||[]).filter(function(x){ return String(x) !== String(val) });
                const input = document.getElementById('input-ufs');
                if(input){
                    const list = input.value.split(',').map(function(s){ return s.trim().toUpperCase() }).filter(Boolean).filter(function(x){ return String(x) !== String(val) });
                    input.value = list.join(', ');
                }
                const cont = document.getElementById('uf-chips'); if(cont){ cont.innerHTML = renderChips(window.state.selectedUFs, 'uf') }
            }
        }catch{}
    }
});

/**
 * Validação básica de CNPJ
 */
function validateCNPJ(c) {
    const s = String(c || '').replace(/\D/g, '');
    if (s.length !== 14) return false;
    if (/^(\d)\1+$/.test(s)) return false;
    const calc = (b) => {
        let soma = 0;
        let p = b.length - 7;
        for (let i = 0; i < b.length; i++) {
            soma += parseInt(b[i], 10) * p--;
            if (p < 2) p = 9
        }
        const resto = soma % 11;
        return (resto < 2) ? 0 : (11 - resto)
    };
    const dv1 = calc(s.slice(0, 12));
    const dv2 = calc(s.slice(0, 13));
    return dv1 === parseInt(s[12], 10) && dv2 === parseInt(s[13], 10)
}

/**
 * Formatação de Moeda BRL
 */
function formatCurrency(num) {
    try {
        const capNum = parseFloat(String(num).replace(/[^0-9.,]/g, '').replace(',', '.')) || 0;
        return Number(capNum).toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' })
    } catch {
        return String(num)
    }
}

/**
 * Função para criar o HTML da Sidebar padronizada
 */
function createSidebarHtml(activePage) {
    const links = [
        { href: 'index.html', label: 'Dashboard', icon: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2V6zm10 0a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2V6zM4 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2v-2zm10 0a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2v-2z"/>' },
        { href: 'cnpj.html', label: 'CNPJ Finder', icon: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 17v-2m3 2v-4m3 4v-6m2 10H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"/>' },
        { href: 'compat.html', label: 'Compatibilidade', icon: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12a9 9 0 01-9 9m9-9a9 9 0 00-9-9m9 9H3m9 9a9 9 0 01-9-9m9 9c1.657 0 3-4.477 3-9s-1.343-9-3-9m0 18v-1m-4 1v-1m8 1v-1m-4 0v-2m0-4h.01M12 7h.01"/>' },
        { href: 'health.html', label: 'Health Check', icon: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 12v1.5L12 21l8-7.5V12M6 12l2-2m6-4l2 2m-2-2l-4-4m4 4v4m4 0h-4m2 0l2 2"/>' }
    ];

    const navLinksHtml = links.map(link => {
        const isActive = activePage.includes(link.href);
        const activeClass = isActive ? ' active' : '';
        return `
            <a href="${link.href}" class="sidebar-link${activeClass} flex items-center gap-3 px-4 py-3 rounded-lg text-slate-400 hover:bg-slate-700/50 transition">
                <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">${link.icon}</svg>
                ${link.label}
            </a>
        `;
    }).join('');

    return `
        <aside class="w-64 bg-slate-800/50 border-r border-slate-700 flex flex-col">
            <div class="p-6 border-b border-slate-700">
                <div class="flex items-center gap-3">
                    <div class="w-10 h-10 rounded-xl bg-gradient-to-br from-indigo-500 to-purple-600 flex items-center justify-center">
                        <svg class="w-6 h-6 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6"/>
                        </svg>
                    </div>
                    <div>
                        <h1 class="font-bold text-lg">Análise Pro</h1>
                        <p class="text-xs text-slate-400">Dados Empresariais</p>
                    </div>
                </div>
            </div>
            <nav class="flex-1 p-4 space-y-2">${navLinksHtml}</nav>
            <div class="p-4 border-t border-slate-700">
                <div class="bg-gradient-to-r from-indigo-600 to-purple-600 rounded-xl p-4">
                    <p class="text-sm font-medium">Plano Premium</p>
                    <p class="text-xs text-indigo-200 mt-1">Consultas Ilimitadas</p>
                    <button class="mt-3 w-full bg-white text-indigo-600 text-sm font-medium py-2 rounded-lg hover:bg-indigo-50 transition">
                        Gerenciar Assinatura
                    </button>
                </div>
            </div>
        </aside>
    `;
}

/**
 * Função para criar o HTML do Header padronizado
 */
function createHeaderHtml() {
    return `
        <header class="bg-slate-800/30 border-b border-slate-700 sticky top-0 z-10">
            <div class="max-w-6xl mx-auto px-6 py-4">
                <div class="flex items-center justify-between">
                    <div class="relative flex-1 max-w-[42rem]">
                        <input type="text" placeholder="Pesquisar CNPJs, segmentos..." class="w-full bg-slate-700/50 border border-slate-600 rounded-lg pl-10 pr-4 py-2.5 text-sm focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500">
                        <svg class="w-5 h-5 text-slate-400 absolute left-3 top-1/2 -translate-y-1/2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"/></svg>
                    </div>
                    <div class="flex items-center gap-4 ml-6">
                        <button class="relative p-2 text-slate-400 hover:text-white transition">
                            <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 17h5l-1.405-1.405A2.032 2.032 0 0118 14.158V11a6.002 6.002 0 00-4-5.659V5a2 2 0 10-4 0v.341C7.67 6.165 6 8.388 6 11v3.159c0 .538-.214 1.055-.595 1.436L4 17h5m6 0v1a3 3 0 11-6 0v-1m6 0H9"/></svg>
                            <span class="absolute top-1 right-1 w-2 h-2 bg-red-500 rounded-full"></span>
                        </button>
                        <div class="flex items-center gap-3 pl-4 border-l border-slate-700">
                            <div class="text-right">
                                <p class="text-sm font-medium">Analista</p>
                                <p class="text-xs text-slate-400">carlos.s</p>
                            </div>
                            <img src="https://api.dicebear.com/7.x/avataaars/svg?seed=Carlos" alt="Avatar" class="w-10 h-10 rounded-full bg-slate-700">
                        </div>
                    </div>
                </div>
            </div>
        </header>
    `;
}

// Inicializa a Sidebar e o Header em todas as páginas
document.addEventListener('DOMContentLoaded', () => {
    if (window.__ENABLE_DEBUG_UI !== true) { return }
    if (!window.browserState) window.browserState = { logs: [] };
    if (!console.__intercepted) {
        const origLog = console.log;
        const origError = console.error;
        const origWarn = console.warn;
        console.log = function(){ window.browserState.logs.push({t:'log',m:Array.from(arguments).join(' '),ts:Date.now()}); origLog.apply(console, arguments) };
        console.error = function(){ window.browserState.logs.push({t:'error',m:Array.from(arguments).join(' '),ts:Date.now()}); origError.apply(console, arguments) };
        console.warn = function(){ window.browserState.logs.push({t:'warn',m:Array.from(arguments).join(' '),ts:Date.now()}); origWarn.apply(console, arguments) };
        console.__intercepted = true;
    }
    if (!document.getElementById('terminal-panel')){
        const btn = document.createElement('button');
        btn.id = 'terminal-toggle';
        btn.className = 'fixed bottom-4 right-4 z-50 bg-slate-800 border border-slate-700 text-slate-200 px-3 py-2 rounded shadow hover:bg-slate-700';
        btn.innerText = 'Logs';
        btn.onclick = function(){ const p = document.getElementById('terminal-panel'); p.classList.toggle('hidden'); if (window.renderTerminal) window.renderTerminal() };
        body.appendChild(btn);
        const panel = document.createElement('div');
        panel.id = 'terminal-panel';
        panel.className = 'hidden fixed bottom-16 right-4 w-[36rem] max-w-[95vw] h-56 bg-slate-900/95 backdrop-blur-sm border border-slate-700 rounded-xl shadow-xl z-50 flex flex-col';
        panel.innerHTML = '<div class="px-4 py-2 border-b border-slate-700 flex items-center justify-between"><div class="text-sm font-semibold">Terminal</div><div class="flex items-center gap-2"><button id="clear-logs" class="text-xs px-2 py-1 bg-slate-700 rounded">Limpar</button><button id="close-terminal" class="text-xs px-2 py-1 bg-slate-700 rounded">Fechar</button></div></div><div id="terminal-content" class="flex-1 overflow-auto text-xs p-3 font-mono"></div>';
        body.appendChild(panel);
        document.getElementById('close-terminal').onclick = function(){ panel.classList.add('hidden') };
        document.getElementById('clear-logs').onclick = function(){ window.browserState.logs = []; renderTerminal() };
        function renderTerminal(){
            const c = document.getElementById('terminal-content');
            const rows = window.browserState.logs.slice(-400).map(x=>{
                const color = x.t==='error' ? 'text-red-400' : (x.t==='warn' ? 'text-yellow-400' : 'text-slate-300');
                return `<div class="${color}">[${new Date(x.ts).toLocaleTimeString()}] ${x.m}</div>`
            }).join('');
            c.innerHTML = rows || '<div class="text-slate-500">Sem logs</div>';
        }
        window.renderTerminal = renderTerminal;
    }
});
