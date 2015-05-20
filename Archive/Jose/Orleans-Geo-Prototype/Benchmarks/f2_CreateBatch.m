fig1=figure;

ax=axes('FontSize', 22,'Parent',fig1);
xlabel('Number of Grains' ,'FontSize',22);
ylabel('Time (milli seconds)' ,'FontSize',22);
%title('HIT RATIO + Random Lookup + quorums-master-config-2-1-random-opt.txt');
hold on; % for matlab 6
X_labels = [1 5 10 20];
%axis(ax, [0 inf 0 inf])

var_1 = [6.451 14.525 25.204 47.560 ];
plot(X_labels, var_1, '-o', 'MarkerSize', 14 , 'LineWidth',  2);

var_3 = [8.107 19.946 36.447 70.610  ];
plot(X_labels, var_3, '--*', 'MarkerSize', 14 , 'LineWidth',  2);

var_5 = [10.823 21.669 40.182 79.324 ];
plot(X_labels, var_5, '--*', 'MarkerSize', 14 , 'LineWidth',  2);

% var_3 = [0.5244 0.7364 0.8152 0.87 0.9262 0.943 0.9598 0.9696 0.9816];
% plot(X_labels, var_3, ':x', 'MarkerSize', 14 , 'LineWidth',  2);
% var_4 = [0.471 0.6516 0.7484 0.8306 0.8742 0.9188 0.943 0.9582 0.9682];
% plot(X_labels, var_4, '--s', 'MarkerSize', 14 , 'LineWidth',  2, 'MarkerFaceColor', [0 0 0], 'MarkerSize', 14);

leg = legend('Batch Create - 1 Server', 'Batch Create - 2 Servers', 'Batch Create - 4 Servers');
set(leg, 'FontSize', 20, 'Location', 'NorthWest');
set(gca,'XTick', X_labels);
set(gca,'XTickLabel', {'1' '5'  '10' '20'});

print -depsc2 f2_Create_Batch.eps
print -djpeg  f2_Create_Batch.jpeg
