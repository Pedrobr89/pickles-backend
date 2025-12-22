/**
 * Gerenciamento de Autenticação
 */

const Auth = {
    user: null,

    async checkAuth() {
        try {
            const response = await fetch('/api/auth/me');
            if (response.ok) {
                const data = await response.json();
                this.user = data.user;
                this.updateUI();
                return true;
            } else {
                return false;
            }
        } catch (error) {
            console.error('Auth check user error:', error);
            return false;
        }
    },

    async login(email, password) {
        try {
            const response = await fetch('/api/auth/login', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ email, password })
            });

            const data = await response.json();

            if (response.ok) {
                this.user = data.user;
                window.location.href = '/app'; // Redireciona para o dashboard
            } else {
                throw new Error(data.error || 'Falha no login');
            }
        } catch (error) {
            throw error;
        }
    },

    async logout() {
        try {
            await fetch('/api/auth/logout', { method: 'POST' });
            this.user = null;
            window.location.href = '/login';
        } catch (error) {
            console.error('Logout error:', error);
        }
    },

    async register(email, password) {
        try {
            const response = await fetch('/api/auth/register', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ email, password })
            });

            const data = await response.json();

            if (!response.ok) {
                throw new Error(data.error || 'Falha no registro');
            }

            return data;
        } catch (error) {
            throw error;
        }
    },

    async forgotPassword(email) {
        try {
            const response = await fetch('/api/auth/reset-password-request', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ email })
            });
            const data = await response.json();
            if (!response.ok) throw new Error(data.error || 'Erro ao redefinir senha');
            return data;
        } catch (error) {
            throw error;
        }
    },

    updateUI() {
        // Atualiza elementos da UI com base no estado de login
        const userDisplay = document.getElementById('user-display');
        if (userDisplay && this.user) {
            userDisplay.textContent = this.user.email;
            userDisplay.style.display = 'block';
        }

        const loginBtn = document.getElementById('btn-login-nav');
        if (loginBtn) loginBtn.style.display = this.user ? 'none' : 'block';

        const logoutBtn = document.getElementById('btn-logout-nav');
        if (logoutBtn) {
            logoutBtn.style.display = this.user ? 'block' : 'none';
            logoutBtn.onclick = () => this.logout();
        }
    },

    requireAuth() {
        // TEMPORÁRIO: Autenticação desabilitada para debug
        console.warn('⚠️ AUTENTICAÇÃO DESABILITADA - Modo debug ativo');
        return; // Não faz nada, permite acesso livre
        /*
        this.checkAuth().then(isAuthenticated => {
            if (!isAuthenticated) {
                window.location.href = '/login';
            }
        });
        */
    }
};

// Export para uso global
window.Auth = Auth;
