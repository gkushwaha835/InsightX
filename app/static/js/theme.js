(function () {
  const STORAGE_KEY = 'insightx-theme';

  function getSavedTheme() {
    try {
      return localStorage.getItem(STORAGE_KEY) === 'dark' ? 'dark' : 'light';
    } catch (err) {
      return 'light';
    }
  }

  function syncButtons(theme) {
    const buttons = document.querySelectorAll('[data-theme-toggle], #themeToggle');
    buttons.forEach((button) => {
      const isDark = theme === 'dark';
      button.setAttribute('aria-pressed', isDark ? 'true' : 'false');
      button.title = isDark ? 'Switch to light mode' : 'Switch to dark mode';
      button.innerHTML = isDark ? '☀️' : '🌙';
    });
  }

  function applyTheme(theme) {
    const root = document.documentElement;
    if (theme === 'dark') {
      root.setAttribute('data-theme', 'dark');
    } else {
      root.removeAttribute('data-theme');
    }
    syncButtons(theme);
  }

  function setTheme(theme) {
    try {
      localStorage.setItem(STORAGE_KEY, theme === 'dark' ? 'dark' : 'light');
    } catch (err) {
      // ignore storage failures
    }
    applyTheme(theme === 'dark' ? 'dark' : 'light');
  }

  function toggleTheme() {
    const nextTheme = document.documentElement.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
    setTheme(nextTheme);
  }

  function initTheme() {
    applyTheme(getSavedTheme());
    document.querySelectorAll('[data-theme-toggle], #themeToggle').forEach((button) => {
      button.addEventListener('click', (event) => {
        event.preventDefault();
        toggleTheme();
      });
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initTheme);
  } else {
    initTheme();
  }

  window.InsightXTheme = {
    getTheme: () => (document.documentElement.getAttribute('data-theme') === 'dark' ? 'dark' : 'light'),
    setTheme,
    toggle: toggleTheme,
  };
  window.toggleTheme = toggleTheme;
})();

