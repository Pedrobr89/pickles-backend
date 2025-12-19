
// --- Chart Helper Functions ---
const _charts = {};

function renderChart(containerId, type, data, options) {
    let container = document.getElementById(containerId);
    if (!container) return;

    let canvas;
    if (container.tagName === 'CANVAS') {
        canvas = container;
    } else {
        // It's a container DIV
        canvas = container.querySelector('canvas');
        if (!canvas) {
            container.innerHTML = ''; // Clear "loading" or text text
            container.classList.remove('flex', 'items-center', 'justify-center', 'text-gray-400'); // Remove placeholder styling
            canvas = document.createElement('canvas');
            canvas.style.width = '100%';
            canvas.style.height = '100%';
            container.appendChild(canvas);
        }
    }

    // If existing chart instance associated with this canvas (or container id logic), destroy it
    // We store charts by containerId to track them effectively
    if (_charts[containerId]) {
        _charts[containerId].destroy();
        delete _charts[containerId];
    }

    try {
        _charts[containerId] = new Chart(canvas, {
            type: type,
            data: data,
            options: Object.assign({
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false }
                }
            }, options || {})
        });
    } catch (e) {
        console.error("Error creating chart for " + containerId, e);
    }
}

function renderGraficoEvolucao(id, labels, values) {
    renderChart(id, 'line', {
        labels: labels,
        datasets: [{
            label: 'Ativas',
            data: values,
            borderColor: '#3b82f6',
            backgroundColor: 'rgba(59, 130, 246, 0.1)',
            borderWidth: 2,
            tension: 0.4,
            fill: true,
            pointRadius: 3
        }]
    }, {
        plugins: { legend: { display: false } },
        scales: {
            x: { grid: { display: false }, ticks: { font: { size: 10 } } },
            y: { grid: { color: '#f1f5f9' }, ticks: { font: { size: 10 } }, beginAtZero: false } // Auto scale
        }
    });
}

function renderGraficoEntradasSaidas(labels, entVals, saiVals) {
    renderChart('vg-entradas-saidas', 'line', { // or bar
        labels: labels,
        datasets: [
            {
                label: 'Entradas',
                data: entVals,
                borderColor: '#22c55e',
                backgroundColor: 'transparent',
                borderWidth: 2,
                tension: 0.4,
                pointRadius: 3
            },
            {
                label: 'Saídas',
                data: saiVals,
                borderColor: '#ef4444',
                backgroundColor: 'transparent',
                borderWidth: 2,
                tension: 0.4,
                pointRadius: 3
            }
        ]
    }, {
        plugins: { legend: { display: true, position: 'top' } },
        scales: {
            x: { grid: { display: false }, ticks: { font: { size: 10 } } },
            y: { grid: { color: '#f1f5f9' }, ticks: { font: { size: 10 } } }
        }
    });
}

function renderGraficoBarra(id, labels, values, label, color) {
    renderChart(id, 'bar', {
        labels: labels,
        datasets: [{
            label: label || 'Valor',
            data: values,
            backgroundColor: color || '#3b82f6',
            borderRadius: 4
        }]
    }, {
        indexAxis: 'y', // Horizontal bar
        plugins: { legend: { display: false } },
        scales: {
            x: { grid: { color: '#f1f5f9' }, ticks: { font: { size: 10 } } },
            y: { grid: { display: false }, ticks: { font: { size: 10 } } }
        }
    });
}

function renderGraficoDonut(id, labels, values, colors) {
    renderChart(id, 'doughnut', {
        labels: labels,
        datasets: [{
            data: values,
            backgroundColor: colors || ['#3b82f6', '#cbd5e1', '#64748b', '#94a3b8'],
            borderWidth: 0
        }]
    }, {
        cutout: '70%',
        plugins: { legend: { display: true, position: 'right', labels: { boxWidth: 10, font: { size: 11 } } } }
    });
}

function renderGraficoRadar(id, labels, datasets) {
    renderChart(id, 'radar', {
        labels: labels,
        datasets: datasets
    }, {
        plugins: { legend: { display: true, position: 'top' } },
        scales: {
            r: {
                beginAtZero: true,
                max: 100,
                ticks: { stepSize: 20, font: { size: 9 } },
                pointLabels: { font: { size: 10 } },
                grid: { color: '#e2e8f0' }
            }
        }
    });
}

function renderGraficoPizza(id, labels, values, colors) {
    renderChart(id, 'pie', {
        labels: labels,
        datasets: [{
            data: values,
            backgroundColor: colors || ['#3b82f6', '#8b5cf6', '#ec4899', '#f59e0b', '#10b981'],
            borderWidth: 2,
            borderColor: '#fff'
        }]
    }, {
        plugins: { legend: { display: true, position: 'right', labels: { boxWidth: 10, font: { size: 11 } } } }
    });
}

