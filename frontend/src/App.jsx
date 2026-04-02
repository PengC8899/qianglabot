import React, { useState } from 'react';
import { Layout, Menu, Button, Card, Form, Input, message } from 'antd';
import { 
  UserOutlined, SendOutlined, 
  HistoryOutlined, StopOutlined, UsergroupAddOutlined
} from '@ant-design/icons';
import { adminLogin } from './api';
import BlacklistManager from './BlacklistManager';
import ProxyManager from './ProxyManager';
import ApiKeyManager from './ApiKeyManager';
import LogViewer from './LogViewer';
import SessionManager from './SessionManager';
import TaskManager from './TaskManager';
import InviteManager from './InviteManager';

const { Header, Content, Sider } = Layout;

function App() {
  const [isLoggedIn, setIsLoggedIn] = useState(Boolean(localStorage.getItem('authToken')));
  const [activeTab, setActiveTab] = useState('tasks');
  const [collapsed, setCollapsed] = useState(false);

  const handleLogin = async (values) => {
    try {
      const res = await adminLogin(values.username, values.password);
      localStorage.setItem('authToken', res.token);
      setIsLoggedIn(true);
      message.success('登录成功');
    } catch (e) {
      message.error('账号或密码错误');
    }
  };

  const handleLogout = () => {
    localStorage.removeItem('authToken');
    setIsLoggedIn(false);
    message.success('已退出登录');
  };

  if (!isLoggedIn) {
    return (
      <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100vh', background: '#f0f2f5' }}>
        <Card title="系统登录" style={{ width: 350, boxShadow: '0 4px 12px rgba(0,0,0,0.1)' }}>
          <Form onFinish={handleLogin}>
            <Form.Item name="username" rules={[{ required: true, message: '请输入账号' }]}>
              <Input prefix={<UserOutlined />} placeholder="账号" size="large" />
            </Form.Item>
            <Form.Item name="password" rules={[{ required: true, message: '请输入密码' }]}>
              <Input.Password placeholder="密码" size="large" />
            </Form.Item>
            <Form.Item style={{ marginBottom: 0 }}>
              <Button type="primary" htmlType="submit" block size="large">登录</Button>
            </Form.Item>
          </Form>
        </Card>
      </div>
    );
  }

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Sider collapsible collapsed={collapsed} onCollapse={setCollapsed}>
        <div className="logo" style={{ height: 32, margin: 16, background: 'rgba(255, 255, 255, 0.2)' }} />
        <Menu 
          theme="dark" 
          defaultSelectedKeys={['tasks']} 
          mode="inline"
          selectedKeys={[activeTab]}
          onClick={(e) => setActiveTab(e.key)}
        >
          <Menu.Item key="sessions" icon={<UserOutlined />}>账号管理</Menu.Item>
          <Menu.Item key="tasks" icon={<SendOutlined />}>任务管理</Menu.Item>
          <Menu.Item key="invite" icon={<UsergroupAddOutlined />}>邀请管理</Menu.Item>
          <Menu.Item key="blacklist" icon={<StopOutlined />}>黑名单</Menu.Item>
          <Menu.Item key="proxies" icon={<StopOutlined />}>代理管理</Menu.Item>
          <Menu.Item key="apikeys" icon={<StopOutlined />}>API Key管理</Menu.Item>
          <Menu.Item key="logs" icon={<HistoryOutlined />}>日志</Menu.Item>
        </Menu>
      </Sider>
      <Layout className="site-layout">
        <Header className="site-layout-background" style={{ padding: '0 24px', background: '#fff', display: 'flex', justifyContent: 'flex-end', alignItems: 'center' }}>
          <Button type="primary" danger onClick={handleLogout}>退出登录</Button>
        </Header>
        <Content style={{ margin: '16px' }}>
          <div className="site-layout-background" style={{ padding: 24, minHeight: 360 }}>
            {activeTab === 'sessions' && <SessionManager />}
            {activeTab === 'tasks' && <TaskManager />}
            {activeTab === 'invite' && <InviteManager />}
            {activeTab === 'blacklist' && <BlacklistManager />}
            {activeTab === 'proxies' && <ProxyManager />}
            {activeTab === 'apikeys' && <ApiKeyManager />}
            {activeTab === 'logs' && <LogViewer />}
          </div>
        </Content>
      </Layout>
    </Layout>
  );
}
export default App;
