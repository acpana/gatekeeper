module.exports = {
  docs: [
    {
      type: 'category',
      label: 'Getting Started',
      collapsed: false,
      items: [
        'intro',
        'install',
        'examples'
      ],
    },
    {
      type: 'category',
      label: 'How to use Gatekeeper',
      collapsed: false,
      items: [
        'howto',
        'audit',
        'violations',
        'sync',
        'exempt-namespaces',
        'library',
        'customize-startup',
        'customize-admission',
        'metrics',
        'debug',
        'emergency',
        'vendor-specific',
        'failing-closed',
        'mutation',
        'constrainttemplates',
        'externaldata',
        'expansion',
        'gator',
        'workload-resources',
        'pubsub'
      ],
    },
    {
      type: 'category',
      label: 'Architecture',
      collapsed: false,
      items: [
        'operations',
        'performance-tuning'
      ],
    },
    {
      type: 'category',
      label: 'Concepts',
      collapsed: false,
      items: ['mutation-background']
    },
    {
      type: 'category',
      label: 'Contributing',
      collapsed: false,
      items: [
        'developers',
        'help',
        'security',
        'pubsub-driver'
      ],
    }
  ]
};
