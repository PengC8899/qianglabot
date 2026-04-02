import React, { useEffect, useState } from 'react';
import { Button, Drawer, Table, Tag } from 'antd';
import { getTaskTargets } from './api';

const TaskDetailDrawer = ({ taskId, onClose }) => {
  const [items, setItems] = useState([]);
  const [loading, setLoading] = useState(false);
  const [filter, setFilter] = useState('all');

  useEffect(() => {
    const loadData = async () => {
      if (!taskId) return;
      setLoading(true);
      try {
        const res = await getTaskTargets(taskId);
        setItems(res.items);
      } finally {
        setLoading(false);
      }
    };
    loadData();
    const interval = setInterval(loadData, 5000);
    return () => clearInterval(interval);
  }, [taskId]);

  const filteredItems = items.filter(item => {
    if (filter === 'all') return true;
    if (filter === 'pending') return item.status === 'pending';
    if (filter === 'success') return item.status === 'success';
    if (filter === 'failed') return item.status === 'failed' || item.status === 'skipped';
    return true;
  });

  const columns = [
    { title: "目标", dataIndex: "target", key: "target" },
    {
      title: "状态",
      dataIndex: "status",
      key: "status",
      render: status => {
        let color = status === 'success' ? 'green' : status === 'failed' ? 'red' : status === 'pending' ? 'orange' : 'default';
        return <Tag color={color}>{status}</Tag>;
      }
    },
    { title: "执行账号", dataIndex: "sender_phone", key: "sender_phone", render: t => t || '-' },
    { title: "执行时间", dataIndex: "executed_at", key: "executed_at", render: t => t ? new Date(t).toLocaleTimeString() : '-' },
    { title: "备注", dataIndex: "error", key: "error", ellipsis: true },
  ];

  return (
    <Drawer
      title={`任务 #${taskId} 详情`}
      placement="right"
      width={700}
      onClose={onClose}
      open={!!taskId}
    >
      <div style={{ marginBottom: 16 }}>
        <div style={{ marginBottom: 8 }}>筛选:</div>
        <Button.Group>
          <Button type={filter === 'all' ? 'primary' : 'default'} onClick={() => setFilter('all')}>全部</Button>
          <Button type={filter === 'pending' ? 'primary' : 'default'} onClick={() => setFilter('pending')}>待发送</Button>
          <Button type={filter === 'success' ? 'primary' : 'default'} onClick={() => setFilter('success')}>成功</Button>
          <Button type={filter === 'failed' ? 'primary' : 'default'} onClick={() => setFilter('failed')}>失败</Button>
        </Button.Group>
        <span style={{ marginLeft: 16, color: '#999' }}>
          共 {items.length} 条，显示 {filteredItems.length} 条
        </span>
      </div>

      <Table
        dataSource={filteredItems}
        columns={columns}
        rowKey="id"
        size="small"
        loading={loading}
        pagination={{ pageSize: 20 }}
      />
    </Drawer>
  );
};

export default TaskDetailDrawer;
