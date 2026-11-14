// Maps the mdBook SUMMARY.md parts onto header topics and their sidebar groups.
export const BASE = '/faucet-stream';

export const TOPICS = [
  {
    id: 'learn', label: 'Learn', icon: 'open-book', link: '/', sections: ['', 'Getting Started', 'Tutorials'],
    groups: ['Start here', 'Understand', 'Tutorials'],
    sectionGroup: { '': 'Start here', 'Getting Started': 'Start here', Tutorials: 'Tutorials' },
    regroup: { 'getting-started/learn': 'Understand' },
  },
  {
    id: 'cookbook', label: 'Cookbook', icon: 'document', link: '/cookbook/', sections: ['Cookbook'],
    lead: 'Short recipes for one job each. Find the task, copy the config, adjust the names.',
  },
  {
    id: 'reference', label: 'Reference', icon: 'information', link: '/reference/', sections: ['Reference'],
    lead: 'Every connector, command and config field, generated from the code where it can be.',
    groups: ['Connectors', 'CLI & config'],
    regroup: {
      'reference/connectors': 'Connectors', 'reference/choosing': 'Connectors', 'reference/conformance': 'Connectors',
      'reference/capability-matrix': 'Connectors', 'reference/compatibility': 'Connectors', 'reference/value-fidelity': 'Connectors',
      'reference/template-hub-matrix': 'Connectors',
      'reference/cli': 'CLI & config', 'reference/config': 'CLI & config', 'reference/editor-setup': 'CLI & config',
      'reference/http-api': 'CLI & config', 'reference/triggers': 'CLI & config',
    },
  },
  {
    id: 'operate', label: 'Operate', icon: 'setting', link: '/operations/', sections: ['Operations'],
    lead: 'Deploy faucet, run it on a schedule, keep it healthy and upgrade it safely.',
    groups: ['Deploy & run', 'Schedule & trigger', 'Keep it healthy', 'Upgrade & maintain'],
    regroup: {
      'operations/deploying': 'Deploy & run', 'cookbook/serve': 'Deploy & run', 'cookbook/web-console': 'Deploy & run',
      'cookbook/mcp': 'Deploy & run', 'cookbook/cluster': 'Deploy & run',
      'cookbook/scheduling': 'Schedule & trigger', 'cookbook/schedules': 'Schedule & trigger', 'cookbook/triggers': 'Schedule & trigger',
      'cookbook/orchestration': 'Schedule & trigger',
      'operations/observability': 'Keep it healthy', 'operations/reliability-testing': 'Keep it healthy',
      'operations/chaos-testing': 'Keep it healthy', 'operations/tuning': 'Keep it healthy',
      'cookbook/troubleshooting': 'Keep it healthy', 'operations/troubleshooting': 'Keep it healthy',
      'operations/pinning': 'Upgrade & maintain', 'operations/upgrading': 'Upgrade & maintain',
      'operations/migrating': 'Upgrade & maintain', 'operations/backup': 'Upgrade & maintain',
    },
  },
  {
    id: 'extend', label: 'Extend', icon: 'puzzle', link: '/extending/', sections: ['Extending'],
    lead: 'Write your own connector, publish it, or embed the faucet engine in another tool.',
    groups: ['Build on faucet'], sectionGroup: { Extending: 'Build on faucet' },
  },
  {
    id: 'compare', label: 'Compare', icon: 'random', link: '/comparison/', sections: ['Comparisons'],
    groups: ['Comparisons'], sectionGroup: { Comparisons: 'Comparisons' },
  },
];

export const GROUP_LEADS = {
  'Shaping the data': 'Transforms, SQL, WASM and topology.',
  'Reading from sources': 'Pagination, auth and discovery.',
  'Moving data reliably': 'State, upserts, CDC, dead-letter queues, rollback.',
  'Data quality & governance': 'Checks, contracts, masking and policies.',
  'Config & reuse': 'Composition, templates, secrets and tuning.',
  'Observability & lineage': 'SLAs, profiling, lineage and costs.',
  Connectors: 'The catalog, tiers, capabilities and value fidelity.',
  'CLI & config': 'Commands, the config file, the HTTP API and triggers.',
  'Deploy & run': 'Containers, Helm, the server, the console and clusters.',
  'Schedule & trigger': 'Cron, schedules, events and orchestrators.',
  'Keep it healthy': 'Metrics, chaos tests, tuning and troubleshooting.',
  'Upgrade & maintain': 'Pinning, upgrades, migrations and backups.',
  'Build on faucet': 'Connectors, the protocol, the marketplace, embedding.',
};
