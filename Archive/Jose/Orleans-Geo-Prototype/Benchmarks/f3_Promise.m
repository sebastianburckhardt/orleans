fig1=figure;

ax=axes('FontSize', 22,'Parent',fig1);
xlabel('Operation length (micro seconds)' ,'FontSize',22);
ylabel('Time (milli seconds' ,'FontSize',22);
%title('HIT RATIO + Random Lookup + quorums-master-config-2-1-random-opt.txt');
hold on; % for matlab 6
unit = 1000;
X_labels = [1*unit 5*unit 10*unit 50*unit 100*unit 500*unit 1000*unit];
%[ 0.005 0.019  0.036 0.178 0.349 1.737 3.476]
%axis(ax, [0 inf 0 inf])

var_0 = [0.092 0.102 0.087 0.069 0.050 0.064 0.057];
plot(log2(X_labels), var_0, '-o', 'MarkerSize', 14 , 'LineWidth',  2);

% var_1 = [1.619 1.612 1.600 1.564];
% plot(X_labels, var_1, '-.+', 'MarkerSize', 14 , 'LineWidth',  2);
% var_2 = [0.5218 0.741 0.8506 0.9096 0.9424 0.9706 0.9792 0.9888 0.989];
% plot(X_labels, var_2, '--*', 'MarkerSize', 14 , 'LineWidth',  2);
% var_3 = [0.5244 0.7364 0.8152 0.87 0.9262 0.943 0.9598 0.9696 0.9816];
% plot(X_labels, var_3, ':x', 'MarkerSize', 14 , 'LineWidth',  2);
% var_4 = [0.471 0.6516 0.7484 0.8306 0.8742 0.9188 0.943 0.9582 0.9682];
% plot(X_labels, var_4, '--s', 'MarkerSize', 14 , 'LineWidth',  2, 'MarkerFaceColor', [0 0 0], 'MarkerSize', 14);

leg = legend('Promise Overhead');
set(leg, 'FontSize', 20, 'Location', 'NorthWest');
set(gca,'XTick', log2(X_labels));
set(gca,'XTickLabel', {'4' '20'  '40' '200', '400' '2K'  '4K'});

print -depsc2 f3_Promise.eps
print -djpeg  f3_Promise.jpeg
