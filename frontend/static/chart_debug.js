// Função auxiliar para adicionar logs de debug
function debugLog(funcName, message, data) {
    if (window.DEBUG_CHARTS) {
        console.log(`[${funcName}] ${message}`, data);
    }
}

// Ativar debug
window.DEBUG_CHARTS = true;

// Função para garantir que sempre temos dados para renderizar
function ensureChartData(labels, values, minItems = 3) {
    if (!labels || labels.length === 0) {
        // Retornar dados de exemplo
        return {
            labels: ['Sem dados', 'Aplique filtros', 'para visualizar'],
            values: [1, 1, 1]
        };
    }
    return { labels, values };
}
