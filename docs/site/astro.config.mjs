import { defineConfig, passthroughImageService } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightSidebarTopics from 'starlight-sidebar-topics';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import rehypeRepoLinks from './plugins/rehype-repo-links.mjs';

const topics = JSON.parse(readFileSync(new URL('./src/topics.json', import.meta.url), 'utf8'));
const SITE = 'https://faucet-hq.github.io';
const BASE = '/faucet-stream';

export default defineConfig({
  site: SITE,
  base: BASE,
  trailingSlash: 'always',
  image: { service: passthroughImageService() },
  markdown: {
    rehypePlugins: [[rehypeRepoLinks, { root: fileURLToPath(new URL('../..', import.meta.url)), repo: 'https://github.com/faucet-hq/faucet-stream' }]],
  },
  integrations: [
    starlight({
      title: 'faucet-stream',
      description: 'The fast, config-driven way to move data.',
      logo: { src: './src/assets/logo-mark.svg', alt: 'faucet-stream' },
      favicon: '/assets/favicon.svg',
      social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/faucet-hq/faucet-stream' }],
      editLink: { baseUrl: 'https://github.com/faucet-hq/faucet-stream/edit/main/docs/site/' },
      lastUpdated: true,
      customCss: ['./src/styles/fonts.css', './src/styles/theme.css'],
      tableOfContents: { minHeadingLevel: 2, maxHeadingLevel: 3 },
      expressiveCode: {
        themes: ['github-light', 'github-dark'],
        styleOverrides: { borderRadius: '10px', codeFontFamily: 'var(--sl-font-mono)', uiFontFamily: 'var(--sl-font)' },
      },
      head: [
        { tag: 'meta', attrs: { name: 'theme-color', content: '#14b8a6' } },
        { tag: 'link', attrs: { rel: 'apple-touch-icon', href: `${BASE}/assets/favicon.png` } },
        { tag: 'meta', attrs: { property: 'og:image', content: `${SITE}${BASE}/assets/social-banner.png` } },
        { tag: 'meta', attrs: { name: 'twitter:card', content: 'summary_large_image' } },
      ],
      components: {
        Header: './src/components/Header.astro',
        SiteTitle: './src/components/SiteTitle.astro',
        PageFrame: './src/components/PageFrame.astro',
        Sidebar: './src/components/Sidebar.astro',
        PageTitle: './src/components/PageTitle.astro',
        Footer: './src/components/Footer.astro',
        ThemeSelect: './src/components/ThemeSelect.astro',
        MobileMenuFooter: './src/components/MobileMenuFooter.astro',
      },
      plugins: [starlightSidebarTopics(topics)],
    }),
  ],
});
