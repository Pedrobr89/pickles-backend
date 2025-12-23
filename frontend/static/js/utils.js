/**
 * Utility functions for formatting and data normalization.
 */

// Format currency to BRL
function formatCurrency(value) {
    try {
        return new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL', maximumFractionDigits: 0 }).format(value);
    } catch {
        return String(value || '');
    }
}

// Get color classes for match score
function getMatchColor(match) {
    if (match >= 90) return 'bg-green-100 text-green-700 border-green-200';
    if (match >= 75) return 'bg-blue-100 text-blue-700 border-blue-200';
    return 'bg-gray-100 text-gray-700 border-gray-200';
}

// Get color classes for company badges
function getBadgeColor(badge) {
    const colors = {
        'Exportadora': 'bg-purple-100 text-purple-700',
        'Crescimento': 'bg-green-100 text-green-700',
        'Consolidada': 'bg-blue-100 text-blue-700',
        'Startup': 'bg-orange-100 text-orange-700',
        'Madura': 'bg-indigo-100 text-indigo-700',
        'Especializada': 'bg-pink-100 text-pink-700'
    };
    return colors[badge] || 'bg-gray-100 text-gray-700';
}

// Normalize company data structure
function normalizeCompany(c) {
    const nomeFantasia = c.nomeFantasia || c.razao || c.razaoSocial || '—';
    const razaoSocial = c.razaoSocial || c.razao || c.nomeFantasia || '—';
    const cnae = (c.cnae || '').toString();
    const cnaeDesc = c.cnaeDesc || c.setor || '';
    const uf = c.uf || '';
    const municipio = c.municipio || '';
    const porte = c.porte || '';
    const idade = (c.idade != null ? c.idade : undefined);
    const match = (c.matchScore != null ? c.matchScore : (c.score != null ? c.score : undefined));
    const badges = Array.isArray(c.badges) ? c.badges : (c.situacao ? [c.situacao] : []);
    const capitalSocial = c.capitalSocial;
    const socios = c.socios;
    return { nomeFantasia, razaoSocial, cnae, cnaeDesc, uf, municipio, porte, idade, match, badges, capitalSocial, socios };
}

// Simple Toast/Notification (if needed, placeholder)
function showToast(message, type = 'info') {
    // Implementation to be added if not found in main.js
    console.log(`[Toast ${type}] ${message}`);
}
