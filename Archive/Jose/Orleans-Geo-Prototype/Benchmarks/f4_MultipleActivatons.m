fig1=figure;

ax=axes('FontSize', 22,'Parent',fig1); %, 'XTick', [100; 1000 ; 5000; 10000; 50000],'YTick', [0:0.1:1]);
xlabel('Number of Activations' ,'FontSize',22);
ylabel('Throughput (requests/second)' ,'FontSize',22);
%title('HIT RATIO + Random Lookup + quorums-master-config-2-1-random-opt.txt');
hold on; % for matlab 6
X_labels = [1 5 10 20];
%axis(ax, [0 inf 0 inf])

var_0 = [338	1835	2245	2235];
plot(X_labels, var_0, '-ks', 'MarkerSize', 20 , 'LineWidth',  3, 'MarkerFaceColor', [0 0 0]);
%plot(X_labels, var_0, '-.+', 'MarkerSize', 14 , 'LineWidth',  2);

var_1 = [152	840	    967	   959];
plot(X_labels, var_1, '-ko', 'MarkerSize', 20 , 'LineWidth',  3);

var_2 = [175 725 1103 1232];
plot(X_labels, var_2, '--*', 'MarkerSize', 20 , 'LineWidth',  3);

var_3 = [97 492 750 1054];
plot(X_labels, var_3, '--x', 'MarkerSize', 20 , 'LineWidth',  3);

% var_4 = [0.471 0.6516 0.7484 0.8306 0.8742 0.9188 0.943 0.9582 0.9682];
% plot(X_labels, var_4, '--s', 'MarkerSize', 14 , 'LineWidth',  2, 'MarkerFaceColor', [0 0 0], 'MarkerSize', 14);

leg = legend('Read 1ms','Write 1ms','Read 5ms','Write 5ms');
set(leg, 'FontSize', 20, 'Location', 'NorthWest');
set(gca,'XTick', X_labels);
set(gca,'XTickLabel', {'1' '5'  '10' '20'});
set(gca,'YTick', [250 500 1000 1500 2000 2500]);
%set(gca,'YTickLabel', {'1' '5'  '10' '20'});
print -depsc2 f4_MultipleActivatons.eps
print -djpeg f4_MultipleActivatons.jpeg

