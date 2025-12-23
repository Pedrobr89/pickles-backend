/**
 * API Client for Backend Interaction
 */
const API = {
    BASE_URL: '/api',

    // Helper to handle responses
    async _fetch(endpoint, options = {}) {
        try {
            const url = endpoint.startsWith('http') ? endpoint : `${this.BASE_URL}${endpoint}`;
            const res = await fetch(url, options);
            if (!res.ok) {
                console.warn(`API Error ${res.status} on ${endpoint}`);
                throw new Error(`API Error: ${res.status}`);
            }
            return await res.json();
        } catch (error) {
            console.error('Fetch error:', error);
            throw error;
        }
    },

    // 1. Visão Geral / Dashboard
    async getAnaliseAtivas() {
        // Matches /api/analise/ativas (implied)
        return this._fetch('/analise/ativas');
    },

    async getAnaliseMercado(filters) {
        // Construct query string from filters
        // filters: { cnae, uf, municipio, porte, ... }
        const params = new URLSearchParams();
        if (filters) {
            Object.keys(filters).forEach(key => {
                if (filters[key]) params.append(key, filters[key]);
            });
        }
        return this._fetch(`/analise/mercado?${params.toString()}`);
    },

    // 2. CNAE Suggestions
    async getCnaeSuggestions(term, setor) {
        if (!term && !setor) return [];
        let url = `/analise/cnaes/sugerir?termo=${encodeURIComponent(term || '')}`;
        if (setor) url += `&setor=${encodeURIComponent(setor)}`;
        const res = await fetch(`/api${url}`); // Direct fetch to match existing pattern or use _fetch
        if (res.ok) {
            const js = await res.json();
            return Array.isArray(js) ? js : (js.data || []);
        }
        return [];
    },

    // 3. CNPJ Search
    async searchCNPJ(term) {
        // term can be CNPJ or Name
        const isCNPJ = term.replace(/[^0-9]/g, '').length === 14;
        const endpoint = isCNPJ
            ? `/cnpj/dados/${term.replace(/[^0-9]/g, '')}`
            : `/cnpj/buscar/${encodeURIComponent(term)}`;
        return this._fetch(endpoint);
    },

    // 4. B2B / Players List
    async getPlayersList(params) {
        // params: { term, uf, porte, idade, match, ... }
        const qs = new URLSearchParams(params).toString();
        return this._fetch(`/analise/players/lista?${qs}`);
    }
};
