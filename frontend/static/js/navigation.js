/**
 * Navigation and Sidebar Logic
 */

let sidebarPinned = true;

function initSidebarState() {
    try {
        const stored = localStorage.getItem('sidebar_pinned');
        if (stored !== null) { sidebarPinned = (stored === 'true'); }
    } catch { }
    applyPinnedState();
}

function applyPinnedState() {
    const sidebar = document.getElementById('sidebar');
    const sidebarBackdrop = document.getElementById('sidebar-backdrop');
    const headerEl = document.getElementById('header');
    const mainContent = document.getElementById('main-content');
    const miniSidebar = document.getElementById('sidebar-mini');
    const pinSidebarBtn = document.getElementById('pin-sidebar');

    if (sidebarPinned) {
        if (sidebar) sidebar.classList.remove('-translate-x-full');
        if (sidebarBackdrop) sidebarBackdrop.classList.add('hidden');
        if (headerEl) {
            headerEl.classList.add('md:ml-72');
            headerEl.classList.remove('md:ml-12');
        }
        if (mainContent) {
            mainContent.classList.add('md:ml-72');
            mainContent.classList.remove('md:ml-12');
        }
        if (miniSidebar) { miniSidebar.classList.add('hidden'); }
        if (pinSidebarBtn) {
            pinSidebarBtn.setAttribute('title', 'Desafixar menu');
            pinSidebarBtn.classList.add('text-blue-600');
        }
    } else {
        if (headerEl) {
            headerEl.classList.remove('md:ml-72');
            headerEl.classList.add('md:ml-12');
        }
        if (mainContent) {
            mainContent.classList.remove('md:ml-72');
            mainContent.classList.add('md:ml-12');
        }
        if (miniSidebar) { miniSidebar.classList.remove('hidden'); }
        if (pinSidebarBtn) {
            pinSidebarBtn.setAttribute('title', 'Fixar menu');
            pinSidebarBtn.classList.remove('text-blue-600');
        }
        closeSidebar();
    }
}

function toggleSidebarPin() {
    sidebarPinned = !sidebarPinned;
    localStorage.setItem('sidebar_pinned', sidebarPinned);
    applyPinnedState();
}

function toggleSidebar() {
    const sidebar = document.getElementById('sidebar');
    const sidebarBackdrop = document.getElementById('sidebar-backdrop');
    if (sidebar) sidebar.classList.toggle('-translate-x-full');
    if (sidebarBackdrop) {
        sidebarBackdrop.classList.toggle('opacity-0');
        sidebarBackdrop.classList.toggle('pointer-events-none');
    }
}

function closeSidebar() {
    const sidebar = document.getElementById('sidebar');
    const sidebarBackdrop = document.getElementById('sidebar-backdrop');
    // On mobile, close always. On desktop, only if not pinned?
    // Using simple logic consistent with existing behavior
    if (window.innerWidth < 768) {
        if (sidebar) sidebar.classList.add('-translate-x-full');
        if (sidebarBackdrop) {
            sidebarBackdrop.classList.add('opacity-0');
            sidebarBackdrop.classList.add('pointer-events-none');
        }
    } else if (!sidebarPinned) {
        if (sidebar) sidebar.classList.add('-translate-x-full');
    }
}

// Initialize on load
document.addEventListener('DOMContentLoaded', () => {
    initSidebarState();

    const pinBtn = document.getElementById('pin-sidebar');
    if (pinBtn) pinBtn.addEventListener('click', toggleSidebarPin);

    const menuBtn = document.getElementById('menu-button');
    if (menuBtn) menuBtn.addEventListener('click', toggleSidebar);

    const closeBtn = document.getElementById('close-sidebar');
    if (closeBtn) closeBtn.addEventListener('click', closeSidebar);

    const backdrop = document.getElementById('sidebar-backdrop');
    if (backdrop) backdrop.addEventListener('click', closeSidebar);

    const miniOpen = document.getElementById('mini-open');
    if (miniOpen) miniOpen.addEventListener('click', () => {
        sidebarPinned = true;
        localStorage.setItem('sidebar_pinned', 'true');
        applyPinnedState();
    });
});
