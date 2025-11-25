import { useEffect, useState } from 'react';
import { 
  Table, Button, message, Tag, Drawer, Space, Alert, Card, 
  Row, Col, Select, Input, Radio 
} from 'antd';
import { 
  TeamOutlined, ClockCircleOutlined, SearchOutlined 
} from '@ant-design/icons';
import dayjs from 'dayjs';

import { useAuth } from '../../context/AuthContext';
import { CourseApiService, SelectionBatchApiService } from '../../client';
import type { 
  CourseResp, 
  StudentResp,
  SelectionBatchResp
} from '../../client';

export const MyCourses = () => {
  const { user } = useAuth();

  // === 状态定义 ===
  const [courses, setCourses] = useState<CourseResp[]>([]);
  const [loading, setLoading] = useState(false);
  
  // 搜索条件
  const [searchCampus, setSearchCampus] = useState<('A'|'B'|'C')[]>(['A', 'B', 'C']); 
  const [searchKeyword, setSearchKeyword] = useState<string>('');
  const [viewType, setViewType] = useState<'my' | 'all'>('my'); // 'my': 我的课程, 'all': 全校课程
  
  // 抽屉状态 (学生名单)
  const [drawerVisible, setDrawerVisible] = useState(false);
  const [currentCourseStudents, setCurrentCourseStudents] = useState<StudentResp[]>([]);
  const [drawerLoading, setDrawerLoading] = useState(false);

  // 批次状态
  const [currentBatch, setCurrentBatch] = useState<SelectionBatchResp | null>(null);
  const [batchLoading, setBatchLoading] = useState(false);

  // === 0. 获取选课批次信息 ===
  const fetchBatches = async () => {
    setBatchLoading(true);
    try {
      const res = await SelectionBatchApiService.getSelectionBatchApiV1SelectionBatchesGet();
      // 找到当前正在进行的批次
      const now = dayjs();
      const activeBatch = res.result.find(batch => {
        // 处理 UTC 时间，假设后端返回的是 UTC ISO 字符串
        // 如果后端没有 Z 后缀，手动加上以确保 dayjs 按 UTC 解析并转为本地时间
        const begin = dayjs(batch.begin_time.endsWith('Z') ? batch.begin_time : batch.begin_time + 'Z');
        const end = dayjs(batch.end_time.endsWith('Z') ? batch.end_time : batch.end_time + 'Z');
        return now.isAfter(begin) && now.isBefore(end);
      });
      
      setCurrentBatch(activeBatch || null);
    } catch (error) {
      console.error("获取批次信息失败", error);
    } finally {
      setBatchLoading(false);
    }
  };

  useEffect(() => {
    fetchBatches();
  }, []);

  // === 1. 获取课程列表 ===
  const fetchCourses = async () => {
    setLoading(true);
    try {
      const res = await CourseApiService.queryCoursesApiV1CoursesGet(
        searchCampus,
        searchKeyword || undefined,
        viewType === 'my' ? user?.user_id : undefined, // 根据视图类型过滤
        undefined,
        undefined
      );
      setCourses(res.result);
    } catch (error) {
      message.error('获取课程失败');
    } finally {
      setLoading(false);
    }
  };

  // 当视图切换或用户登录后自动刷新
  useEffect(() => { 
    if (user) fetchCourses(); 
  }, [user, viewType]); // 监听 viewType 变化

  // === 2. 查看学生名单 (只读) ===
  const handleViewStudents = async (courseId: number) => {
    setDrawerVisible(true);
    setDrawerLoading(true);
    try {
      const res = await CourseApiService.getCourseStudentsApiV1CoursesCourseIdStudentsGet(courseId);
      setCurrentCourseStudents(res.result);
    } catch (error) {
      message.error('获取学生名单失败');
    } finally {
      setDrawerLoading(false);
    }
  };

  const columns = [
    { title: 'ID', dataIndex: 'course_id', width: 80 },
    { title: '课程名称', dataIndex: 'name' },
    { 
      title: '校区', 
      dataIndex: 'campus', 
      width: 80,
      render: (text: string) => {
        const colors = { A: 'blue', B: 'green', C: 'orange' };
        // @ts-ignore
        return <Tag color={colors[text]}>{text}</Tag>;
      }
    },
    { title: '容量', width: 100, render: (r: CourseResp) => `${r.num_selected} / ${r.capacity}` },
    { title: '授课教师', dataIndex: 'teachers', ellipsis: true }, // 显示教师名字，因为可能是全校课程
    {
      title: '',
      width: 180,
      align: 'right' as const,
      render: (_: any, record: CourseResp) => (
        <Space>
          <Button icon={<TeamOutlined />} onClick={() => handleViewStudents(record.course_id)}>
            查看名单
          </Button>
        </Space>
      )
    }
  ];

  // 计算剩余时间等显示信息
  const renderBatchStatus = () => {
    if (batchLoading) return null;
    
    if (currentBatch) {
      const end = dayjs(currentBatch.end_time.endsWith('Z') ? currentBatch.end_time : currentBatch.end_time + 'Z');
      return (
        <Alert
          message={
            <Space>
              <ClockCircleOutlined />
              <span style={{ fontWeight: 'bold' }}>当前选课批次: {currentBatch.name}</span>
              <Tag color="success">进行中</Tag>
            </Space>
          }
          description={`选课开放时间：${dayjs(currentBatch.begin_time.endsWith('Z') ? currentBatch.begin_time : currentBatch.begin_time + 'Z').format('YYYY-MM-DD HH:mm')} ~ ${end.format('YYYY-MM-DD HH:mm')} (当前允许学生选课)`}
          type="success"
          showIcon
          style={{ marginBottom: 16 }}
        />
      );
    } else {
      return (
        <Alert
          message="当前非选课时段"
          description="目前没有进行中的选课批次。"
          type="info"
          showIcon
          style={{ marginBottom: 16 }}
        />
      );
    }
  };

  return (
    <div>
      {/* 状态栏 */}
      {renderBatchStatus()}

      {/* 搜索区域 */}
      <Card style={{ marginBottom: 16 }} bodyStyle={{ padding: '16px 24px' }}>
        <Row gutter={16} align="middle">
          <Col>
            <Radio.Group 
              value={viewType} 
              onChange={e => setViewType(e.target.value)} 
              buttonStyle="solid"
            >
              <Radio.Button value="my">我的课程</Radio.Button>
              <Radio.Button value="all">全校课程</Radio.Button>
            </Radio.Group>
          </Col>
          <Col>
            <Select
              mode="multiple"
              placeholder="选择校区"
              style={{ width: 180 }}
              value={searchCampus}
              onChange={setSearchCampus}
              options={[
                { label: 'A 校区', value: 'A' },
                { label: 'B 校区', value: 'B' },
                { label: 'C 校区', value: 'C' },
              ]}
            />
          </Col>
          <Col>
            <Input 
              placeholder="搜索课程名称" 
              style={{ width: 200 }}
              value={searchKeyword}
              onChange={e => setSearchKeyword(e.target.value)}
              onPressEnter={fetchCourses}
            />
          </Col>
          <Col>
            <Button type="primary" icon={<SearchOutlined />} onClick={fetchCourses}>查询</Button>
          </Col>
        </Row>
      </Card>

      <Table 
        dataSource={courses} 
        columns={columns} 
        rowKey="course_id" 
        loading={loading} 
      />

      {/* 学生名单抽屉 (纯查看模式) */}
      <Drawer
        title="选课学生名单"
        placement="right"
        onClose={() => setDrawerVisible(false)}
        open={drawerVisible}
        width={600} 
      >
        <Table
          loading={drawerLoading}
          dataSource={currentCourseStudents}
          rowKey="stu_id"
          pagination={false}
          columns={[
            { title: '学号', dataIndex: 'stu_id' },
            { title: '姓名', dataIndex: 'name' },
            { title: '性别', dataIndex: 'sex', width: 60 },
            { title: '校区', dataIndex: 'current_campus', width: 80 },
          ]}
        />
      </Drawer>
    </div>
  );
};