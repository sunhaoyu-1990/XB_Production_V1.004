# coding=gbk

import numpy as np
import pickle
import scipy.special
import Keyword_and_Parameter as kp
import Document_process as dop

'''
    �������ݣ�������ṹ��������ģ���㷨���������������ʼ�������������㡢�������򴫵ݡ�����ѵ�����̺�����ѵ��׼ȷ�ʼ���
    ����ʱ�䣺2022/3/22
    ���ʱ�䣺2022/3/25
'''


class neuralNetwork:
    # ��ʼ��������
    def __init__(self, inputnodes, hiddennodes, outputnodes, learningrate):
        # �����������3�ֲ㣬ÿ�����Ԫ������
        self.inodes = inputnodes  # �������Ԫ����
        self.hnodes = hiddennodes  # ������Ԫ����
        self.onodes = outputnodes  # �������Ԫ����
        # ����������ز�֮���Ȩ�ؾ����״ν���������ɣ���ΧΪ ����һ���������Ŀ����ŵĵ�����������
        self.wih = np.random.normal(0.0, pow(self.hnodes, -0.5), (self.hnodes, self.inodes))
        # ���ز��������֮���Ȩ�ؾ����״ν���������ɣ���ΧΪ ����һ���������Ŀ����ŵĵ�����������
        self.who = np.random.normal(0.0, pow(self.onodes, -0.5), (self.onodes, self.hnodes))
        self.activation_function = lambda x: scipy.special.expit(x)  # ��ʼ�������
        self.inverse_activation_function = lambda x: scipy.special.logit(x)  # ��ʼ�����򼤻��
        # ѧϰ����
        self.lr = learningrate
        pass

    # ѵ��������
    def train(self, inputs_list, targets_list):
        # 1.��Ը�����ѵ�������������
        # �����������Ŀ������ֱ�ת����2ά������ʽ
        inputs = np.array(inputs_list, ndmin=2).T
        targets = np.array(targets_list, ndmin=2).T
        # �õ����ز����������
        hidden_inputs = np.dot(self.wih, inputs)
        # �õ����ز�����
        hidden_outputs = self.activation_function(hidden_inputs)
        # �õ�����������
        final_inputs = np.dot(self.who, hidden_outputs)
        # �õ����������
        final_outputs = self.activation_function(final_inputs)

        # 2.�������������Ż�Ȩ��
        # ���������
        output_errors = targets - final_outputs
        # �������ز��Ȩ�ص����
        hidden_errors = np.dot(self.who.T, output_errors)
        # �õ������ز㵽�����֮���Ȩ�صı仯ֵ����������һ�ε�Ȩ�أ�����transpose�ǽ�2ά����ת����1ά
        self.who += self.lr * np.dot(output_errors * final_outputs * (1 - final_outputs), np.transpose(hidden_outputs))
        # �õ�������㵽���ز�֮���Ȩ�صı仯ֵ����������һ�ε�Ȩ�أ�����transpose�ǽ�2ά����ת����1ά
        self.wih += self.lr * np.dot(hidden_errors * hidden_outputs * (1 - hidden_outputs), np.transpose(inputs))
        pass

    # ��ѯ����������
    def query(self, inputs_list):
        # ��������������飬ת����Ϊ2ά��ʽ
        inputs = np.array(inputs_list, ndmin=2).T
        # ���ز���������ݣ�ΪȨ�ؾ��������������
        hidden_inputs = np.dot(self.wih, inputs)
        # ���ز��������ݣ�Ϊ���ز���������ݽ���sigmoid����
        hidden_outputs = self.activation_function(hidden_inputs)
        # �������������ݣ�ΪȨ�ؾ��������ز���������
        final_inputs = np.dot(self.who, hidden_outputs)
        # ������������ݣ�Ϊ�������������ݽ���sigmoid����
        final_outputs = self.activation_function(final_inputs)

        return final_inputs

    # ����������Լ���Ŀ�꼯�����׼ȷ��
    def score(self, inputs_list, targets_list):
        scorecard = []
        for i in range(len(inputs_list)):
            output_values = neuralNetwork.query(inputs_list[i])
            output_value = np.argmax(output_values)
            if targets_list[i] == output_value:
                scorecard.append(1)
            else:
                scorecard.append(0)
        scorecard = np.asarray(scorecard)
        return scorecard.sum() / scorecard.size

    # backquery the neural network
    # we'll use the same termnimology to each item,
    # eg target are the values at the right of the network, albeit used as input
    # eg hidden_output is the signal to the right of the middle nodes
    def backquery(self, targets_list):
        # transpose the targets list to a vertical array
        final_outputs = np.array(targets_list, ndmin=2).T

        # calculate the signal into the final output layer
        final_inputs = self.inverse_activation_function(final_outputs)

        # calculate the signal out of the hidden layer
        hidden_outputs = np.dot(self.who.T, final_inputs)
        # scale them back to 0.01 to .99
        hidden_outputs -= np.min(hidden_outputs)
        hidden_outputs /= np.max(hidden_outputs)
        hidden_outputs *= 0.98
        hidden_outputs += 0.01

        # calculate the signal into the hidden layer
        hidden_inputs = self.inverse_activation_function(hidden_outputs)

        # calculate the signal out of the input layer
        inputs = np.dot(self.wih.T, hidden_inputs)
        # scale them back to 0.01 to .99
        inputs -= np.min(inputs)
        inputs /= np.max(inputs)
        inputs *= 0.98
        inputs += 0.01

        return inputs


class neuralNetwork_2:

    # ��ʼ��������
    def __init__(self, inputnodes, outputnodes, hnodes_num, hnodes_num_list, learningrate, activation_type):
        # �����������3�ֲ㣬ÿ�����Ԫ������
        self.inodes = inputnodes  # �������Ԫ����
        self.onodes = outputnodes  # �������Ԫ����
        self.hnodes_num = hnodes_num

        # �жϽ��м�����������
        self.hnodes = hnodes_num_list  # ������Ԫ����
        # ����������ز�֮���Ȩ�ؾ����״ν���������ɣ���ΧΪ ����һ���������Ŀ����ŵĵ�����������
        self.wih = np.random.normal(0.0, pow(self.hnodes[0], -0.5), (self.hnodes[0], self.inodes))
        # ���ز��������֮���Ȩ�ؾ����״ν���������ɣ���ΧΪ ����һ���������Ŀ����ŵĵ�����������
        self.who = np.random.normal(0.0, pow(self.onodes, -0.5), (self.onodes, self.hnodes[-1]))
        # ����������ز�֮���ƫ�þ����״ν���������ɣ���ΧΪ -1��+1֮��
        self.bih = np.random.normal(0.0, 0.5, (self.hnodes[0], 1))
        # ���ز��������֮���ƫ�þ����״ν���������ɣ���ΧΪ -1��+1֮��
        self.bho = np.random.normal(0.0, 0.5, (self.onodes, 1))

        # ���ز�֮���Ȩ�ؾ���
        self.whh = []
        self.bhh = []
        for i in range(hnodes_num - 1):
            # ���ز������ز�֮���ƫ�þ����״ν���������ɣ���ΧΪ -1��+1֮��
            self.whh.append(np.random.normal(0.0, pow(self.onodes, -0.5), (self.hnodes[i + 1], self.hnodes[i])))
            # ���ز������ز�֮���ƫ�þ����״ν���������ɣ���ΧΪ -1��+1֮��
            self.bhh.append(np.random.normal(0.0, 0.5, (self.hnodes[i + 1], 1)))

        # �趨�����������
        self.activation_type = activation_type

        if activation_type == 'sigmoid':
            self.activation_function = lambda x: scipy.special.expit(x)  # ��ʼ�������,������õ���sigmoid����
        elif activation_type == 'relu':
            self.activation_function = lambda x: np.maximum(x, 0)  # ��ʼ�������,������õ���Relu����

        self.inverse_activation_function = lambda x: scipy.special.logit(x)  # ��ʼ�����򼤻��

        # ѧϰ����
        self.lr = learningrate
        pass

    # ѵ��������
    def train(self, inputs_list, targets_list):
        # 1.��Ը�����ѵ�������������
        # �����������Ŀ������ֱ�ת����2ά������ʽ
        inputs = np.array(inputs_list, ndmin=2).T
        targets = np.array(targets_list, ndmin=2).T

        # �м�����֮������������
        hiddens_inputs = []
        hiddens_outputs = []
        for i in range(len(self.whh) + 1):
            if i == 0:
                # �õ����ز����������
                hidden_inputs = np.dot(self.wih, inputs) + self.bih
            else:
                # ���к�һ��������������ݼ���
                hidden_inputs = np.dot(self.whh[i - 1], hidden_outputs) + self.bhh[i - 1]
            # ���к�һ�������������ݼ��㣬���ڿ������������ÿһ������ļ����
            hidden_outputs = self.activation_function(hidden_inputs)
            # ��ÿ�ε���������������ݽ��зֱ𱣴�
            hiddens_inputs.append(hidden_inputs)
            hiddens_outputs.append(hidden_outputs)

        # �õ�����������
        final_inputs = np.dot(self.who, hidden_outputs) + self.bho
        # �õ����������
        final_outputs = self.activation_function(final_inputs)

        # 2.�������������Ż�Ȩ��
        # ���������
        output_errors = targets - final_outputs

        # �м�����֮������������
        hiddens_errors = []
        for i in range(len(self.whh) + 1):
            if i == 0:
                hidden_errors = np.dot(self.who.T, output_errors)
            else:
                hidden_errors = np.dot(self.whh[len(self.whh) - i].T, hidden_errors)
            hiddens_errors.append(hidden_errors)

        # ���򴫵�����
        self.back_propagation(output_errors, hiddens_errors, final_outputs, hiddens_outputs, inputs, final_inputs,
                              hiddens_inputs)

        pass

    # ��ѯ����������
    def query(self, inputs_list):
        # ��������������飬ת����Ϊ2ά��ʽ
        inputs = np.array(inputs_list, ndmin=2).T

        for i in range(len(self.whh) + 1):
            if i == 0:
                # �õ����ز����������
                hidden_inputs = np.dot(self.wih, inputs) + self.bih
            else:
                # ���к�һ��������������ݼ���
                hidden_inputs = np.dot(self.whh[i - 1], hidden_outputs) + self.bhh[i - 1]
            # ���к�һ�������������ݼ���
            hidden_outputs = self.activation_function(hidden_inputs)

        # �������������ݣ�ΪȨ�ؾ��������ز���������
        final_inputs = np.dot(self.who, hidden_outputs) + self.bho
        # ������������ݣ�Ϊ�������������ݽ���sigmoid����
        final_outputs = self.activation_function(final_inputs)

        return final_outputs

    # ����������Լ���Ŀ�꼯�����׼ȷ��
    def score(self, inputs_list, targets_list, result_type):
        scorecard = []
        recall = []
        for i in range(len(inputs_list)):
            if result_type == 'list':
                output_values = neuralNetwork.query(inputs_list[i])
                output_value = np.argmax(output_values)
            else:
                if output_values[0][0] >= 0.5:
                    output_value = 1
                else:
                    output_value = 0
            if targets_list[i] == output_value:
                scorecard.append(1)
            else:
                scorecard.append(0)
            if targets_list[i] == 1 and output_value == 1:
                recall.append(1)
            elif targets_list[i] == 1 and output_value != 1:
                recall.append(0)
        scorecard = np.asarray(scorecard)
        recall = np.asarray(recall)
        score = (2 * (scorecard.sum() / scorecard.size) * (recall.sum() / recall.size)) / ((scorecard.sum() / scorecard.size) + (recall.sum() / recall.size))
        return score

    # eg hidden_output is the signal to the right of the middle nodes
    def backquery(self, targets_list):
        # transpose the targets list to a vertical array
        final_outputs = np.array(targets_list, ndmin=2).T

        # calculate the signal into the final output layer
        final_inputs = self.inverse_activation_function(final_outputs)

        for i in range(len(self.whh) + 1):
            if i == 0:
                # �õ����ز����������
                hidden_outputs = np.dot(self.who.T, final_inputs - self.bho)
                hidden_outputs -= np.min(hidden_outputs)
                hidden_outputs /= np.max(hidden_outputs)
                hidden_outputs *= 0.98
                hidden_outputs += 0.01
            else:
                # ���к�һ��������������ݼ���
                hidden_outputs = np.dot(self.whh[len(self.whh) - i].T, hidden_inputs - self.bhh[len(self.whh) - i])
                hidden_outputs -= np.min(hidden_outputs)
                hidden_outputs /= np.max(hidden_outputs)
                hidden_outputs *= 0.98
                hidden_outputs += 0.01
            # ���к�һ�������������ݼ��㣬���ڿ������������ÿһ������ļ����
            hidden_inputs = self.inverse_activation_function(hidden_outputs)

        # calculate the signal out of the input layer
        inputs = np.dot(self.wih.T, hidden_inputs)
        # scale them back to 0.01 to .99
        inputs -= np.min(inputs)
        inputs /= np.max(inputs)
        inputs *= 0.98
        inputs += 0.01

        return inputs

    # Relu��������
    def ReluPrime(self, x):
        x[x > 0] = 1
        x[x <= 0] = self.lr
        return x

    # change by 2022/6/5
    def save_result(self, network_save_path=''):
        para_save = {'inputnodes': self.inodes, 'outputnodes': self.onodes, 'hnodes_num': self.hnodes_num,
                     'hnodes_num_list': self.hnodes, 'learningrate': self.lr, 'activation_type': self.activation_type}
        if network_save_path == '':
            network_save_path = kp.get_parameter_with_keyword('netword_save_path')
        save_name = ['initial', 'who', 'whh', 'wih', 'bho', 'bhh', 'bih']
        save_data = [para_save, self.who, self.whh, self.wih, self.bho, self.bhh, self.bih]
        for i in range(len(save_name)):
            output_hal = open(network_save_path + save_name[i] + '.pkl', 'wb')
            str = pickle.dumps(save_data[i])
            output_hal.write(str)
            output_hal.close()

    # change by 2022/6/5
    def load_paremeter(self, path=''):
        if path == '':
            network_save_path = dop.path_of_holder_document(kp.get_parameter_with_keyword('network_save_path'))
        else:
            network_save_path = dop.path_of_holder_document(path)
        for i in range(len(network_save_path)):
            if network_save_path[i][-3:] == 'csv':
                continue
            with open(network_save_path[i], 'rb') as file:
                data = pickle.loads(file.read())
            # if network_save_path[i].rsplit('/', 1)[1][:-4] == 'initial':
            #     self.inodes = data['inputnodes']  # �������Ԫ����
            #     self.onodes = data['outputnodes']  # �������Ԫ����
            #     self.hnodes_num = data['hnodes_num']
            #     # �жϽ��м�����������
            #     self.hnodes = data['hnodes_num_list']
            #     self.activation_type = data['activation_type']
            #     self.lr = data['learningrate']

            if network_save_path[i].rsplit('/', 1)[1][:-4] == 'who':
                self.who = data

            elif network_save_path[i].rsplit('/', 1)[1][:-4] == 'whh':
                self.whh = data

            elif network_save_path[i].rsplit('/', 1)[1][:-4] == 'wih':
                self.wih = data

            elif network_save_path[i].rsplit('/', 1)[1][:-4] == 'bho':
                self.bho = data

            elif network_save_path[i].rsplit('/', 1)[1][:-4] == 'bhh':
                self.bhh = data

            elif network_save_path[i].rsplit('/', 1)[1][:-4] == 'bih':
                self.bih = data

    # ���򴫵�����
    def back_propagation(self, output_errors, hiddens_errors, final_outputs, hiddens_outputs, inputs, final_inputs,
                         hiddens_inputs):
        if self.activation_type == 'sigmoid':
            for i in range(len(hiddens_errors)):
                if i == 0:
                    # �õ������ز㵽�����֮���Ȩ�صı仯ֵ����������һ�ε�Ȩ�أ�����transpose�ǽ�2ά����ת����1ά
                    self.who += self.lr * np.dot(output_errors * final_outputs * (1 - final_outputs),
                                                 np.transpose(hiddens_outputs[len(hiddens_errors) - i - 1]))

                    # �õ�������㵽���ز�֮���Ȩ�صı仯ֵ����������һ�ε�Ȩ�أ�����transpose�ǽ�2ά����ת����1ά
                    self.wih += self.lr * np.dot(hiddens_errors[-1] * hiddens_outputs[i] * (1 - hiddens_outputs[i]),
                                                 np.transpose(inputs))

                    # �õ������ز㵽�����֮���Ȩ�صı仯ֵ����������һ�ε�Ȩ�أ�����transpose�ǽ�2ά����ת����1ά
                    self.bho += self.lr * output_errors * final_outputs * (1 - final_outputs)

                    # �õ�������㵽���ز�֮���Ȩ�صı仯ֵ����������һ�ε�Ȩ�أ�����transpose�ǽ�2ά����ת����1ά
                    self.bih += self.lr * hiddens_errors[-1] * hiddens_outputs[i] * (1 - hiddens_outputs[i])
                else:
                    self.whh[len(hiddens_errors) - i - 1] += self.lr * np.dot(
                        hiddens_errors[i - 1] * hiddens_outputs[len(hiddens_errors) - i] * (1 - hiddens_outputs[len(hiddens_errors) - i]),
                        np.transpose(hiddens_outputs[len(hiddens_errors) - i - 1]))

                    self.bhh[len(hiddens_errors) - i - 1] += self.lr * hiddens_errors[i - 1] * hiddens_outputs[
                        len(hiddens_errors) - i] * (1 - hiddens_outputs[len(hiddens_errors) - i])

        elif self.activation_type == 'relu':
            # �õ������ز㵽�����֮���Ȩ�صı仯ֵ����������һ�ε�Ȩ�أ�����transpose�ǽ�2ά����ת����1ά
            # self.who += self.lr * np.dot(output_errors * self.ReluPrime(final_inputs), np.transpose(hidden_outputs))
            #
            # # �õ�������㵽���ز�֮���Ȩ�صı仯ֵ����������һ�ε�Ȩ�أ�����transpose�ǽ�2ά����ת����1ά
            # self.wih += self.lr * np.dot(hidden_errors * self.ReluPrime(hidden_inputs), np.transpose(inputs))
            #
            # # �õ������ز㵽�����֮���Ȩ�صı仯ֵ����������һ�ε�Ȩ�أ�����transpose�ǽ�2ά����ת����1ά
            self.bho += self.lr * output_errors * self.ReluPrime(final_inputs)
            #
            # # �õ�������㵽���ز�֮���Ȩ�صı仯ֵ����������һ�ε�Ȩ�أ�����transpose�ǽ�2ά����ת����1ά
            # self.bih += self.lr * hidden_errors * self.ReluPrime(hidden_inputs)
