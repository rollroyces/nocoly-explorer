// Mock data for the playground. Each worksheet has rows, schema, and a partition key.

const PG_DATA = {
  customers: {
    id: 'ws_customers',
    name: 'Customers',
    totalRows: 487,
    pageSize: 200, // overridable
    partitionBy: 'region',
    columns: [
      { name: 'id',          type: 'int64',    nullable: false },
      { name: 'name',        type: 'string',   nullable: false },
      { name: 'region',      type: 'string',   nullable: false },
      { name: 'status',      type: 'string',   nullable: false },
      { name: 'email',       type: 'string',   nullable: true },
      { name: 'created_at',  type: 'timestamp', nullable: false },
      { name: '_updatedAt',  type: 'timestamp', nullable: false },
    ],
    partitions: {
      'region=HK': 243,
      'region=SZ': 244,
    },
    rows: [
      { id: 1,  name: 'Acme Corp',          region: 'HK', status: 'Active', email: 'contact@acme.hk',     created_at: '2024-03-12T08:14:00Z', _updatedAt: '2025-04-21T11:02:00Z' },
      { id: 2,  name: 'Bright Logistics',   region: 'SZ', status: 'Active', email: null,                 created_at: '2024-07-05T13:45:00Z', _updatedAt: '2025-05-02T09:18:00Z' },
      { id: 3,  name: 'Cedar Trading',      region: 'HK', status: 'Active', email: 'desk@cedar.hk',       created_at: '2024-01-22T10:30:00Z', _updatedAt: '2025-03-19T07:55:00Z' },
      { id: 4,  name: 'Delta Manufacturing',region: 'SZ', status: 'Inactive',email: 'ops@delta.cn',        created_at: '2023-11-08T16:00:00Z', _updatedAt: '2024-12-14T12:00:00Z' },
      { id: 5,  name: 'Evergreen Holdings', region: 'HK', status: 'Active', email: 'info@evergreen.hk',   created_at: '2024-09-30T09:00:00Z', _updatedAt: '2025-06-01T15:33:00Z' },
    ],
  },

  orders: {
    id: 'ws_orders',
    name: 'Orders',
    totalRows: 1240,
    pageSize: 200,
    partitionBy: 'created_date',
    granularity: 'day',
    columns: [
      { name: 'order_id',     type: 'int64',     nullable: false },
      { name: 'customer_id',  type: 'int64',     nullable: false },
      { name: 'amount',       type: 'double',    nullable: false },
      { name: 'currency',     type: 'string',    nullable: false },
      { name: 'region',       type: 'string',    nullable: false },
      { name: 'status',       type: 'string',    nullable: false },
      { name: 'created_date', type: 'date',      nullable: false },
      { name: '_updatedAt',   type: 'timestamp', nullable: false },
    ],
    partitions: {
      'created_date=2025-04-21': 312,
      'created_date=2025-04-22': 298,
      'created_date=2025-04-23': 340,
      'created_date=2025-04-24': 290,
    },
    rows: [
      { order_id: 10001, customer_id: 1,    amount: 12500.50, currency: 'HKD', region: 'HK', status: 'Active', created_date: '2025-04-21', _updatedAt: '2025-04-21T10:14:00Z' },
      { order_id: 10002, customer_id: 2,    amount:  8420.00, currency: 'CNY', region: 'SZ', status: 'Active', created_date: '2025-04-21', _updatedAt: '2025-04-21T11:02:00Z' },
      { order_id: 10003, customer_id: 1,    amount: 33000.00, currency: 'HKD', region: 'HK', status: 'Active', created_date: '2025-04-22', _updatedAt: '2025-04-22T09:18:00Z' },
      { order_id: 10004, customer_id: 3,    amount:  1250.75, currency: 'HKD', region: 'HK', status: 'Active', created_date: '2025-04-22', _updatedAt: '2025-04-22T14:45:00Z' },
      { order_id: 10005, customer_id: 5,    amount: 18750.00, currency: 'HKD', region: 'HK', status: 'Active', created_date: '2025-04-23', _updatedAt: '2025-04-23T08:30:00Z' },
    ],
  },

  inventory: {
    id: 'ws_inventory',
    name: 'Inventory',
    totalRows: 8032,
    pageSize: 200,
    partitionBy: 'warehouse',
    columns: [
      { name: 'sku',          type: 'string', nullable: false },
      { name: 'warehouse',    type: 'string', nullable: false },
      { name: 'quantity',     type: 'int64',  nullable: false },
      { name: 'reorder_at',   type: 'int64',  nullable: true },
      { name: 'last_counted', type: 'date',   nullable: false },
    ],
    partitions: {
      'warehouse=HKG1': 2003,
      'warehouse=SZX2': 1998,
      'warehouse=PVG3': 2015,
      'warehouse=PEK4': 2016,
    },
    rows: [
      { sku: 'SKU-001', warehouse: 'HKG1', quantity: 145, reorder_at: 50,  last_counted: '2025-04-15' },
      { sku: 'SKU-002', warehouse: 'SZX2', quantity:  72, reorder_at: 30,  last_counted: '2025-04-15' },
      { sku: 'SKU-003', warehouse: 'HKG1', quantity:   8, reorder_at: 20,  last_counted: '2025-04-14' },
      { sku: 'SKU-004', warehouse: 'PVG3', quantity: 220, reorder_at: 100, last_counted: '2025-04-16' },
      { sku: 'SKU-005', warehouse: 'PEK4', quantity:  12, reorder_at: 25,  last_counted: '2025-04-16' },
    ],
  },

  incidents: {
    id: 'ws_incidents',
    name: 'Incidents',
    totalRows: 25000,
    pageSize: 200,
    partitionBy: 'severity',
    columns: [
      { name: 'incident_id', type: 'int64',     nullable: false },
      { name: 'severity',    type: 'string',    nullable: false },
      { name: 'category',    type: 'string',    nullable: false },
      { name: 'tags',        type: 'list<string>', nullable: true },
      { name: 'resolved',    type: 'bool',      nullable: false },
      { name: 'opened_at',   type: 'timestamp', nullable: false },
    ],
    partitions: {
      'severity=critical':  1200,
      'severity=high':      4800,
      'severity=medium':   11500,
      'severity=low':       7500,
    },
    rows: [
      { incident_id: 90001, severity: 'critical', category: 'auth',    tags: ['login', 'mfa'],     resolved: false, opened_at: '2025-04-21T03:14:00Z' },
      { incident_id: 90002, severity: 'high',     category: 'payment', tags: ['timeout'],           resolved: true,  opened_at: '2025-04-21T08:22:00Z' },
      { incident_id: 90003, severity: 'medium',   category: 'ui',      tags: ['button', 'mobile'],  resolved: false, opened_at: '2025-04-21T11:45:00Z' },
      { incident_id: 90004, severity: 'low',      category: 'docs',    tags: ['typo'],               resolved: true,  opened_at: '2025-04-21T14:00:00Z' },
      { incident_id: 90005, severity: 'critical', category: 'data',    tags: ['export', 'csv'],      resolved: false, opened_at: '2025-04-21T18:09:00Z' },
    ],
  },
};
