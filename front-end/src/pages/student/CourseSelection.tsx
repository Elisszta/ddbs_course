import { useEffect, useState } from 'react';
import { 
  Table, Button, message, Tag, Space, Card, Row, Col, Select, Input, Radio, Alert, Popconfirm
} from 'antd';
import { 
  ClockCircleOutlined, SearchOutlined, CheckCircleOutlined, CloseCircleOutlined
} from '@ant-design/icons';
import dayjs from 'dayjs';

import { useAuth } from '../../context/AuthContext';
import { CourseApiService, SelectionBatchApiService } from '../../client';
import type { 
  CourseResp, 
  SelectionBatchResp
} from '../../client';

export const CourseSelection = () => {
  const { user } = useAuth();

  // === 状态定义 ===
  const [courses, setCourses] = useState<CourseResp[]>([]);
  const [loading, setLoading] = useState(false);
  
  // 搜索条件
  const [searchCampus, setSearchCampus] = useState<('A'|'B'|'C')[]>(['A', 'B', 'C']); 
  const [searchKeyword, setSearchKeyword] = useState<string>('');
  // 学生视图：'all' (全校可选课程) vs 'my' (已选课程)
  const [viewType, setViewType] = useState<'all' | 'my'>('all');
  
  // 批次状态
  const [currentBatch, setCurrentBatch] = useState<SelectionBatchResp | null>(null);
  const [batchLoading, setBatchLoading] = useState(false);

  // === 0. 获取选课批次信息 ===
  const fetchBatches = async () => {
    setBatchLoading(true);
    try {
      const res = await SelectionBatchApiService.getSelectionBatchApiV1SelectionBatchesGet();
      const now = dayjs();
      const activeBatch = res.result.find(batch => {
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
        undefined, // teacher: 学生不需要按老师过滤
        undefined, // only_not_full
        viewType === 'my' ? true : undefined, // only_selected: 如果是'my'视图，只查已选
        // 不需要手动传 userId，后端会自动识别当前登录用户。
      );
      setCourses(res.result);
    } catch (error) {
      message.error('获取课程失败');
    } finally {
      setLoading(false);
    }
  };

  // 初始加载和条件变化时刷新
  useEffect(() => { 
    if (user) fetchCourses(); 
  }, [user, viewType]); // 监听 viewType 变化

  // === 2. 选课操作 ===
  const handleSelect = async (courseId: number) => {
    if (!currentBatch) {
      message.warning('当前不在选课时间内，无法选课');
      return;
    }
    try {
      // 注意：虽然接口名为 selectCourse...Post，但参数里可能有 stu_id。
      // 如果是学生自己选课，通常 stu_id 可以不传（后端取当前用户），或者传自己的 ID。
      await CourseApiService.selectCourseApiV1CoursesCourseIdSelectPost(courseId);
      message.success('抢课成功');
      fetchCourses(); // 刷新列表状态
    } catch (error: any) {
      const errorMsg = error.body?.detail?.msg || error.body?.detail || '选课失败';
      message.error(errorMsg);
    }
  };

  // === 3. 退课操作 ===
  const handleDeselect = async (courseId: number) => {
    if (!currentBatch) {
      message.warning('当前不在选课时间内，无法退课');
      return;
    }
    try {
      await CourseApiService.deselectCourseApiV1CoursesCourseIdDeselectPost(courseId);
      message.success('退课成功');
      fetchCourses(); // 刷新列表
    } catch (error: any) {
      const errorMsg = error.body?.detail?.msg || error.body?.detail || '退课失败';
      message.error(errorMsg);
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
    { title: '授课教师', dataIndex: 'teachers', ellipsis: true },
    {
      title: '状态',
      width: 100,
      render: (_: any, record: CourseResp) => {
        if (record.is_selected) {
          return <Tag icon={<CheckCircleOutlined />} color="success">已选</Tag>;
        }
        if (record.num_selected >= record.capacity) {
          return <Tag icon={<CloseCircleOutlined />} color="error">已满</Tag>;
        }
        return <Tag color="default">未选</Tag>;
      }
    },
    {
      title: '',
      width: 120,
      align: 'right' as const,
      render: (_: any, record: CourseResp) => {
        // 只有在选课期间才允许操作
        const disabled = !currentBatch;
        
        if (record.is_selected) {
          return (
            <Popconfirm 
              title="确定要退选这门课吗？" 
              onConfirm={() => handleDeselect(record.course_id)}
              okText="退课"
              cancelText="取消"
              disabled={disabled}
            >
              <Button danger size="small" disabled={disabled}>退课</Button>
            </Popconfirm>
          );
        } else {
          return (
            <Button 
              type="primary" 
              size="small" 
              onClick={() => handleSelect(record.course_id)}
              disabled={disabled || record.num_selected >= record.capacity}
            >
              选课
            </Button>
          );
        }
      }
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
          description={`选课开放时间：${dayjs(currentBatch.begin_time.endsWith('Z') ? currentBatch.begin_time : currentBatch.begin_time + 'Z').format('YYYY-MM-DD HH:mm')} ~ ${end.format('YYYY-MM-DD HH:mm')}`}
          type="success"
          showIcon
          style={{ marginBottom: 16 }}
        />
      );
    } else {
      return (
        <Alert
          message="当前非选课时段"
          description="目前没有进行中的选课批次，暂时无法进行选课或退课操作。"
          type="warning"
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
              <Radio.Button value="all">全校课程</Radio.Button>
              <Radio.Button value="my">已选课程</Radio.Button>
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
    </div>
  );
};